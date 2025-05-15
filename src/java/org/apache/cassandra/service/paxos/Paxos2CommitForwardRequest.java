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

import java.io.IOException;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.paxos.Commit.Agreed;

import static org.apache.cassandra.dht.AbstractBounds.tokenSerializer;

/**
 * Request to forward a Paxos V2 commit operation to a replica coordinator.
 * This is used when the original coordinator is not a replica but needs to
 * execute a Paxos commit for a tracked keyspace that requires MutationId generation.
 * 
 * Contains only the essential data needed by PaxosCommit instead of the full Participants object.
 */
public class Paxos2CommitForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final Agreed commit;
    public final ConsistencyLevel consistencyForConsensus;
    public final ConsistencyLevel consistencyForCommit;
    public final EndpointsForToken all;
    public final EndpointsForToken allLive;
    public final EndpointsForToken allDown;
    public final int required;
    public final boolean isUrgent;

    public Paxos2CommitForwardRequest(Agreed commit,
                                     ConsistencyLevel consistencyForConsensus,
                                     ConsistencyLevel consistencyForCommit,
                                     EndpointsForToken all,
                                     EndpointsForToken allLive,
                                     EndpointsForToken allDown,
                                     int required,
                                     boolean isUrgent)
    {
        this.commit = commit;
        this.consistencyForConsensus = consistencyForConsensus;
        this.consistencyForCommit = consistencyForCommit;
        this.all = all;
        this.allLive = allLive;
        this.allDown = allDown;
        this.required = required;
        this.isUrgent = isUrgent;
    }

    public static class Serializer implements IVersionedSerializer<Paxos2CommitForwardRequest>
    {
        @Override
        public void serialize(Paxos2CommitForwardRequest request, DataOutputPlus out, int version) throws IOException
        {
            Agreed.serializer.serialize(request.commit, out, version);
            out.writeByte(request.consistencyForConsensus.code);
            out.writeByte(request.consistencyForCommit.code);
            
            // Serialize EndpointsForToken collections
            serializeEndpoints(request.all, out, version);
            serializeEndpoints(request.allLive, out, version);
            serializeEndpoints(request.allDown, out, version);
            
            out.writeInt(request.required);
            out.writeBoolean(request.isUrgent);
        }

        @Override
        public Paxos2CommitForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Agreed commit = Agreed.serializer.deserialize(in, version);
            ConsistencyLevel consistencyForConsensus = ConsistencyLevel.fromCode(in.readUnsignedByte());
            ConsistencyLevel consistencyForCommit = ConsistencyLevel.fromCode(in.readUnsignedByte());
            
            // Deserialize EndpointsForToken collections using partitioner from commit
            EndpointsForToken all = deserializeEndpoints(in, version, commit.getPartitionUpdate().metadata().partitioner);
            EndpointsForToken allLive = deserializeEndpoints(in, version, commit.getPartitionUpdate().metadata().partitioner);
            EndpointsForToken allDown = deserializeEndpoints(in, version, commit.getPartitionUpdate().metadata().partitioner);
            
            int required = in.readInt();
            boolean isUrgent = in.readBoolean();
            
            return new Paxos2CommitForwardRequest(commit, consistencyForConsensus, consistencyForCommit, 
                                                  all, allLive, allDown, required, isUrgent);
        }

        @Override
        public long serializedSize(Paxos2CommitForwardRequest request, int version)
        {
            long size = Agreed.serializer.serializedSize(request.commit, version)
                        + 1  // consistencyForConsensus.code
                        + 1; // consistencyForCommit.code
            
            size += endpointsSerializedSize(request.all, version);
            size += endpointsSerializedSize(request.allLive, version);
            size += endpointsSerializedSize(request.allDown, version);
            
            size += TypeSizes.INT_SIZE; // required
            size += TypeSizes.BOOL_SIZE; // isUrgent
            
            return size;
        }
        
        private void serializeEndpoints(EndpointsForToken endpoints, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(endpoints.size());
            Token token = endpoints.token();
            Token.compactSerializer.serialize(token, out, version);
            
            for (Replica replica : endpoints)
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(replica.endpoint(), out, version);
                tokenSerializer.serialize(replica.range(), out, version);
                out.writeBoolean(replica.isFull());
            }
        }
        
        private EndpointsForToken deserializeEndpoints(DataInputPlus in, int version, IPartitioner partitioner) throws IOException
        {
            int size = in.readInt();
            Token token = Token.compactSerializer.deserialize(in, partitioner, version);
            
            EndpointsForToken.Builder builder = EndpointsForToken.builder(token);
            for (int i = 0; i < size; i++)
            {
                InetAddressAndPort endpoint = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
                Range<Token> range = (Range<Token>) tokenSerializer.deserialize(in, partitioner, version);
                boolean isFull = in.readBoolean();
                builder.add(new Replica(endpoint, range, isFull));
            }
            return builder.build();
        }
        
        private long endpointsSerializedSize(EndpointsForToken endpoints, int version)
        {
            long size = TypeSizes.INT_SIZE; // size
            size += Token.compactSerializer.serializedSize(endpoints.token(), version); // token
            
            for (Replica replica : endpoints)
            {
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(replica.endpoint(), version);
                size += tokenSerializer.serializedSize(replica.range(), version);
                size += TypeSizes.BOOL_SIZE; // isFull
            }
            return size;
        }
    }
}