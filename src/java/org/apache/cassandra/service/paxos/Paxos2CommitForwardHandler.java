/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.paxos;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static org.apache.cassandra.utils.concurrent.ConditionAsConsumer.newConditionAsConsumer;

/**
 * Handler for forwarded Paxos V2 commit requests.
 * Executes the commit operation on behalf of the original coordinator,
 * ensuring that MutationId generation happens on a replica coordinator.
 */
public class Paxos2CommitForwardHandler implements IVerbHandler<Paxos2CommitForwardRequest>
{
    public static final Paxos2CommitForwardHandler instance = new Paxos2CommitForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(Paxos2CommitForwardHandler.class);

    @Override
    public void doVerb(Message<Paxos2CommitForwardRequest> message)
    {
        Paxos2CommitForwardRequest request = message.payload;
        
        Tracing.trace("Executing forwarded Paxos V2 commit for {}", request.commit.getPartitionUpdate().partitionKey());

        try
        {
            Commit.Agreed commitToExecute = request.commit;
            
            // Generate proper mutation ID for tracked keyspaces
            String keyspaceName = request.commit.getPartitionUpdate().metadata().keyspace;
            KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(keyspaceName);
            
            if (ksMetadata != null && ksMetadata.params.replicationType.isTracked())
            {
                Token token = request.commit.getPartitionUpdate().partitionKey().getToken();
                MutationId mutationId = MutationTrackingService.instance.nextMutationId(keyspaceName, token);
                
                // Create commit with proper mutation ID
                Mutation mutationWithId = request.commit.makeMutation(mutationId);
                commitToExecute = new Commit.Agreed(request.commit.ballot, mutationWithId);
            }

            // Execute the commit operation using the updated PaxosCommit.commit method
            ConditionAsConsumer<PaxosCommit.Status> onDone = newConditionAsConsumer();
            PaxosCommit.Status[] statusHolder = new PaxosCommit.Status[1];
            
            // Capture the status when the callback is invoked
            Consumer<PaxosCommit.Status> statusCapture = status -> {
                statusHolder[0] = status;
                onDone.accept(status);
            };
            
            // Use PaxosCommit.commit with extracted endpoint data
            PaxosCommit.commit(commitToExecute,
                              request.all,
                              request.allLive, 
                              request.allDown,
                              request.required,
                              request.isUrgent,
                              request.consistencyForConsensus,
                              request.consistencyForCommit,
                              false, // allowHints
                              statusCapture);
            
            // Wait for completion
            try
            {
                onDone.awaitUntil(System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30));
                PaxosCommit.Status status = statusHolder[0];
                
                if (status != null && status.isSuccess())
                {
                    // Send success response back to original coordinator
                    MessagingService.instance().respond(NoPayload.noPayload, message);
                }
                else
                {
                    logger.error("Forwarded Paxos V2 commit failed with status: {}", status);
                    MessagingService.instance().respondWithFailure(org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN, message);
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                logger.error("Forwarded Paxos V2 commit interrupted", e);
                MessagingService.instance().respondWithFailure(org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN, message);
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to execute forwarded Paxos V2 commit for {}", request.commit, e);
            MessagingService.instance().respondWithFailure(org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN, message);
        }
    }
}