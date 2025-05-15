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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.paxos.Paxos.Participants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static java.util.Collections.emptyMap;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_COMMIT_REMOTE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.service.StorageProxy.shouldHint;
import static org.apache.cassandra.service.StorageProxy.submitHint;
import static org.apache.cassandra.service.paxos.Commit.Agreed;
import static org.apache.cassandra.utils.concurrent.ConditionAsConsumer.newConditionAsConsumer;

// Does not support EACH_QUORUM, as no such thing as EACH_SERIAL
public class PaxosCommit<OnDone extends Consumer<? super PaxosCommit.Status>> extends PaxosRequestCallback<NoPayload>
{
    public static final RequestHandler requestHandler = new RequestHandler();
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommit.class);

    private static volatile boolean ENABLE_DC_LOCAL_COMMIT = CassandraRelevantProperties.ENABLE_DC_LOCAL_COMMIT.getBoolean();

    public static boolean getEnableDcLocalCommit()
    {
        return ENABLE_DC_LOCAL_COMMIT;
    }

    public static void setEnableDcLocalCommit(boolean enableDcLocalCommit)
    {
        ENABLE_DC_LOCAL_COMMIT = enableDcLocalCommit;
    }

    /**
     * Represents the current status of a commit action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        private final Paxos.MaybeFailure maybeFailure;

        Status(Paxos.MaybeFailure maybeFailure)
        {
            this.maybeFailure = maybeFailure;
        }

        boolean isSuccess() { return maybeFailure == null; }
        Paxos.MaybeFailure maybeFailure() { return maybeFailure; }

        public String toString() { return maybeFailure == null ? "Success" : maybeFailure.toString(); }
    }

    private static final Status success = new Status(null);

    private static final AtomicLongFieldUpdater<PaxosCommit> responsesUpdater = AtomicLongFieldUpdater.newUpdater(PaxosCommit.class, "responses");

    final Agreed commit;
    final boolean allowHints;
    final ConsistencyLevel consistencyForConsensus;
    final ConsistencyLevel consistencyForCommit;

    final EndpointsForToken replicas;
    final int required;
    final OnDone onDone;

    // Mutation tracking fields for tracked keyspaces
    final MutationId mutationId;
    final org.agrona.collections.IntHashSet remoteReplicas;

    /**
     * packs two 32-bit integers;
     * bit 00-31: accepts
     * bit 32-63: failures/timeouts
     * 
     * {@link #accepts} 
     * {@link #failures}
     */
    private volatile long responses;

    public PaxosCommit(Agreed commit, boolean allowHints, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, EndpointsForToken replicas, int required, OnDone onDone)
    {
        // Check if this is a tracked keyspace and generate mutation ID if needed
        String keyspaceName = commit.getPartitionUpdate().metadata().keyspace;
        KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(keyspaceName);
        boolean isTracked = ksMetadata != null && ksMetadata.params.replicationType.isTracked();
        
        MutationId mutationId = null;
        Agreed commitToUse = commit;
        org.agrona.collections.IntHashSet remoteReplicas = null;
        
        if (isTracked)
        {
            // Generate mutation ID for tracked keyspace
            org.apache.cassandra.dht.Token token = commit.getPartitionUpdate().partitionKey().getToken();
            mutationId = MutationTrackingService.instance.nextMutationId(keyspaceName, token);
            
            // Create commit with proper mutation ID
            org.apache.cassandra.db.Mutation mutationWithId = commit.makeMutation(mutationId);
            commitToUse = new Commit.Agreed(commit.ballot, mutationWithId);
            
            // Collect remote replicas for tracking service
            remoteReplicas = new org.agrona.collections.IntHashSet();
            ClusterMetadata metadata = ClusterMetadata.current();
            for (int i = 0; i < replicas.size(); i++)
            {
                Replica replica = replicas.get(i);
                if (!replica.isSelf())
                {
                    remoteReplicas.add(metadata.directory.peerId(replica.endpoint()).id());
                }
            }
        }
        
        this.commit = commitToUse;
        this.allowHints = allowHints;
        this.consistencyForConsensus = consistencyForConsensus;
        this.consistencyForCommit = consistencyForCommit;
        this.replicas = replicas;
        this.onDone = onDone;
        this.required = required;
        this.mutationId = mutationId;
        this.remoteReplicas = remoteReplicas;
        
        if (required == 0)
            onDone.accept(status());
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static Paxos.Async<Status> commit(Agreed commit, EndpointsForToken all, EndpointsForToken allLive, EndpointsForToken allDown, int required, boolean isUrgent, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints)
    {
        // Check if this is a tracked keyspace requiring forwarding to a replica coordinator
        if (isTrackedKeyspaceRequiringForwarding(commit, all))
        {
            // For async version, create a wrapper that handles forwarding
            Status[] statusHolder = new Status[1];
            ConditionAsConsumer<Status> condition = newConditionAsConsumer();
            Consumer<Status> statusCapture = status -> {
                statusHolder[0] = status;
                condition.accept(status);
            };
            forwardPaxos2Commit(commit, all, allLive, allDown, required, isUrgent, consistencyForConsensus, consistencyForCommit, statusCapture);
            
            return new Paxos.Async<Status>()
            {
                @Override
                public Status awaitUntil(long deadline)
                {
                    try
                    {
                        condition.awaitUntil(deadline);
                        return statusHolder[0] != null ? statusHolder[0] : new Status(new Paxos.MaybeFailure(true, all.size(), required, 0, emptyMap()));
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return new Status(new Paxos.MaybeFailure(true, all.size(), required, 0, emptyMap()));
                    }
                }
            };
        }

        // to avoid unnecessary object allocations we extend PaxosPropose to implements Paxos.Async
        class Async extends PaxosCommit<ConditionAsConsumer<Status>> implements Paxos.Async<Status>
        {
            private Async(Agreed commit, boolean allowHints, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, EndpointsForToken all, int required)
            {
                super(commit, allowHints, consistencyForConsensus, consistencyForCommit, all, required, newConditionAsConsumer());
            }

            public Status awaitUntil(long deadline)
            {
                try
                {
                    onDone.awaitUntil(deadline);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return new Status(new Paxos.MaybeFailure(true, replicas.size(), required, 0, emptyMap()));
                }

                return status();
            }
        }

        Async async = new Async(commit, allowHints, consistencyForConsensus, consistencyForCommit, all, required);
        async.start(allLive, allDown, isUrgent, false);
        return async;
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static <T extends Consumer<Status>> T commit(Agreed commit, EndpointsForToken all, EndpointsForToken allLive, EndpointsForToken allDown, int required, boolean isUrgent, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints, T onDone)
    {
        // Check if this is a tracked keyspace requiring forwarding to a replica coordinator
        if (isTrackedKeyspaceRequiringForwarding(commit, all))
        {
            forwardPaxos2Commit(commit, all, allLive, allDown, required, isUrgent, consistencyForConsensus, consistencyForCommit, onDone);
            return onDone;
        }

        new PaxosCommit<>(commit, allowHints, consistencyForConsensus, consistencyForCommit, all, required, onDone)
                .start(allLive, allDown, isUrgent, true);
        return onDone;
    }

    static Paxos.Async<Status> commit(Agreed commit, Participants participants, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints)
    {
        return commit(commit, participants.all, participants.allLive, participants.allDown, 
                     participants.requiredFor(consistencyForCommit), participants.isUrgent(),
                     consistencyForConsensus, consistencyForCommit, allowHints);
    }

    static <T extends Consumer<Status>> T commit(Agreed commit, Participants participants, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints, T onDone)
    {
        return commit(commit, participants.all, participants.allLive, participants.allDown, 
                     participants.requiredFor(consistencyForCommit), participants.isUrgent(),
                     consistencyForConsensus, consistencyForCommit, allowHints, onDone);
    }

    /**
     * Send commit messages to peers (or self)
     */
    void start(EndpointsForToken allLive, EndpointsForToken allDown, boolean isUrgent, boolean async)
    {
        boolean executeOnSelf = false;
        Message<Agreed> commitMessage = Message.out(PAXOS_COMMIT_REQ, commit, isUrgent);

        Message<Mutation> mutationMessage = null;
        if (ENABLE_DC_LOCAL_COMMIT && consistencyForConsensus.isDatacenterLocal())
            mutationMessage = Message.out(PAXOS2_COMMIT_REMOTE_REQ, commit.makeMutation(), isUrgent);

        for (int i = 0, mi = allLive.size(); i < mi ; ++i)
            executeOnSelf |= isSelfOrSend(commitMessage, mutationMessage, allLive.endpoint(i));

        for (int i = 0, mi = allDown.size(); i < mi ; ++i)
            onFailure(allDown.endpoint(i), RequestFailure.NODE_DOWN);

        // Register write request with tracking service for tracked keyspaces
        if (mutationId != null && remoteReplicas != null && !remoteReplicas.isEmpty())
        {
            MutationTrackingService.instance.sentWriteRequest(commit.makeMutation(), remoteReplicas);
        }

        if (executeOnSelf)
        {
            ExecutorPlus executor = PAXOS_COMMIT_REQ.stage.executor();
            if (async) executor.execute(this::executeOnSelf);
            else executor.maybeExecuteImmediately(this::executeOnSelf);
        }
    }

    /**
     * If isLocal return true; otherwise if the destination is alive send our message, and if not mark the callback with failure
     */
    private boolean isSelfOrSend(Message<Agreed> commitMessage, Message<Mutation> mutationMessage, InetAddressAndPort destination)
    {
        if (shouldExecuteOnSelf(destination))
            return true;

        // don't send commits to remote dcs for local_serial operations
        if (mutationMessage != null && !isInLocalDc(destination))
            MessagingService.instance().sendWithCallback(mutationMessage, destination, this);
        else
            MessagingService.instance().sendWithCallback(commitMessage, destination, this);

        return false;
    }

    private static boolean isInLocalDc(InetAddressAndPort destination)
    {
        Locator locator = DatabaseDescriptor.getLocator();
        return locator.local().sameDatacenter(locator.location(destination));
    }

    /**
     * Record a failure or timeout, and maybe submit a hint to {@code from}
     */
    @Override
    public void onFailure(InetAddressAndPort from, RequestFailure reason)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} {} from {}", commit, reason, from);

        // Track failed response for tracked keyspaces
        if (mutationId != null)
        {
            MutationTrackingService.instance.retryFailedWrite(mutationId, from, reason);
        }

        response(false, from);
        Replica replica = replicas.lookup(from);

        if (allowHints && shouldHint(replica))
            submitHint(commit.makeMutation(), replica, null);
    }

    /**
     * Record a success response
     */
    public void onResponse(Message<NoPayload> response)
    {
        logger.trace("{} Success from {}", commit, response.from());

        // Track successful response for tracked keyspaces 
        // (Local mutations are witnessed from Keyspace.applyInternalTracked)
        if (mutationId != null && response != null)
        {
            MutationTrackingService.instance.receivedWriteResponse(mutationId, response.from());
        }

        response(true, response.from());
    }

    /**
     * Execute locally and record response
     */
    public void executeOnSelf()
    {
        executeOnSelf(commit, RequestHandler::execute);
    }

    @Override
    public void onResponse(NoPayload response, InetAddressAndPort from)
    {
        // Track successful response for tracked keyspaces
        if (mutationId != null && response != null)
        {
            MutationTrackingService.instance.receivedWriteResponse(mutationId, from);
        }
        
        response(response != null, from);
    }

    /**
     * Record a failure or success response if {@code from} contributes to our consistency.
     * If we have reached a final outcome of the commit, run {@code onDone}.
     */
    private void response(boolean success, InetAddressAndPort from)
    {
        if (consistencyForCommit.isDatacenterLocal() && !InOurDc.endpoints().test(from))
            return;

        long responses = responsesUpdater.addAndGet(this, success ? 0x1L : 0x100000000L);
        // next two clauses mutually exclusive to ensure we only invoke onDone once, when either failed or succeeded
        if (accepts(responses) == required) // if we have received _precisely_ the required accepts, we have succeeded
            onDone.accept(status());
        else if (replicas.size() - failures(responses) == required - 1) // if we are _unable_ to receive the required accepts, we have failed
            onDone.accept(status());
    }

    /**
     * @return the Status as of now, which may be final or may indicate we have not received sufficient responses
     */
    Status status()
    {
        long responses = this.responses;
        if (isSuccessful(responses))
            return success;

        return new Status(new Paxos.MaybeFailure(replicas.size(), required, accepts(responses), failureReasonsAsMap()));
    }

    private boolean isSuccessful(long responses)
    {
        return accepts(responses) >= required;
    }

    private static int accepts(long responses)
    {
        return (int) (responses & 0xffffffffL);
    }

    private static int failures(long responses)
    {
        return (int) (responses >>> 32);
    }

    public static class RequestHandler implements IVerbHandler<Agreed>
    {
        @Override
        public void doVerb(Message<Agreed> message)
        {
            NoPayload response = execute(message.payload);
            // NOTE: for correctness, this must be our last action, so that we cannot throw an error and send both a response and a failure response
            if (response == null)
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
            else
                MessagingService.instance().respond(response, message);
        }

        private static NoPayload execute(Agreed agreed)
        {
            if (!Paxos.isInRangeAndShouldProcess(agreed.getPartitionUpdate().partitionKey(), agreed.getPartitionUpdate().metadata(), false))
                return null;

            PaxosState.commitDirect(agreed);
            Tracing.trace("Enqueuing acknowledge to {}", agreed.ballot);
            return NoPayload.noPayload;
        }
    }

    /**
     * Checks if this commit needs to be forwarded to a replica coordinator for tracked keyspace support.
     */
    private static boolean isTrackedKeyspaceRequiringForwarding(Agreed commit, EndpointsForToken all)
    {
        // Get keyspace metadata from the commit's table metadata
        String keyspaceName = commit.getPartitionUpdate().metadata().keyspace;
        org.apache.cassandra.schema.KeyspaceMetadata ksMetadata = org.apache.cassandra.schema.Schema.instance.getKeyspaceMetadata(keyspaceName);
        
        if (ksMetadata == null || !ksMetadata.params.replicationType.isTracked())
            return false;
            
        // Check if current coordinator is not a replica
        InetAddressAndPort localEndpoint = org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort();
        boolean isLocalReplica = all.endpoints().contains(localEndpoint);
        return !isLocalReplica;
    }

    /**
     * Forwards a Paxos V2 commit operation to a replica coordinator for tracked keyspaces.
     */
    private static <T extends Consumer<Status>> void forwardPaxos2Commit(Agreed commit, 
                                                                         EndpointsForToken all,
                                                                         EndpointsForToken allLive,
                                                                         EndpointsForToken allDown,
                                                                         int required,
                                                                         boolean isUrgent,
                                                                         ConsistencyLevel consistencyForConsensus,
                                                                         ConsistencyLevel consistencyForCommit,
                                                                         T onDone)
    {
        InetAddressAndPort localEndpoint = org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort();
        
        // Find first live replica to forward to
        InetAddressAndPort replicaCoordinator = null;
        for (InetAddressAndPort endpoint : all.endpoints())
        {
            if (!endpoint.equals(localEndpoint) && allLive.contains(endpoint))
            {
                replicaCoordinator = endpoint;
                break;
            }
        }
        
        if (replicaCoordinator == null)
        {
            // No live replica available
            onDone.accept(new Status(new Paxos.MaybeFailure(false, all.size(), required, 0, emptyMap())));
            return;
        }
        
        // Create forward request with extracted participant data
        Paxos2CommitForwardRequest forwardRequest = new Paxos2CommitForwardRequest(commit, consistencyForConsensus, consistencyForCommit, 
                                                                                   all, allLive, allDown,
                                                                                   required, isUrgent);
        org.apache.cassandra.net.Message<Paxos2CommitForwardRequest> message = org.apache.cassandra.net.Message.out(org.apache.cassandra.net.Verb.PAXOS2_COMMIT_FORWARD_REQ, forwardRequest);
        
        // Create callback to handle forwarding response
        org.apache.cassandra.net.RequestCallback<org.apache.cassandra.net.NoPayload> callback = new org.apache.cassandra.net.RequestCallback<org.apache.cassandra.net.NoPayload>()
        {
            @Override
            public void onResponse(org.apache.cassandra.net.Message<org.apache.cassandra.net.NoPayload> response)
            {
                onDone.accept(success);
            }

            @Override
            public void onFailure(InetAddressAndPort from, org.apache.cassandra.exceptions.RequestFailure reason)
            {
                onDone.accept(new Status(new Paxos.MaybeFailure(false, all.size(), required, 0, emptyMap())));
            }
        };
        
        try
        {
            org.apache.cassandra.net.MessagingService.instance().sendWithCallback(message, replicaCoordinator, callback);
        }
        catch (Exception e)
        {
            onDone.accept(new Status(new Paxos.MaybeFailure(false, all.size(), required, 0, emptyMap())));
        }
    }

}
