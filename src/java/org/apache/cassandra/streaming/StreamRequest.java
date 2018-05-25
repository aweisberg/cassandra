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
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;

public class StreamRequest
{
    public static final IVersionedSerializer<StreamRequest> serializer = new StreamRequestSerializer();

    public final String keyspace;
    public final Replicas replicas;
    public final Collection<String> columnFamilies = new HashSet<>();
    public StreamRequest(String keyspace, Replicas replicas, Collection<String> columnFamilies)
    {
        this.keyspace = keyspace;
        this.replicas = replicas;
        this.columnFamilies.addAll(columnFamilies);
    }

    public static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest>
    {
        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeInt(request.replicas.size());
            for (Replica replica : request.replicas)
            {
                MessagingService.validatePartitioner(replica.getRange());
                CompactEndpointSerializationHelper.streamingInstance.serialize(replica.getEndpoint(), out, version);
                Token.serializer.serialize(replica.getRange().left, out, version);
                Token.serializer.serialize(replica.getRange().right, out, version);
                out.writeBoolean(replica.isFull());
            }
            out.writeInt(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        public StreamRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            int replicaCount = in.readInt();
            Replicas replicas = new ReplicaList(replicaCount);
            for (int i = 0; i < replicaCount; i++)
            {
                //TODO, super need to review the usage of streaming vs not streaming endpoint serialization helper
                //to make sure I'm not using the wrong one some of the time, like do repair messages use the
                //streaming version?
                InetAddressAndPort endpoint = CompactEndpointSerializationHelper.streamingInstance.deserialize(in, version);
                Token left = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), version);
                Token right = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), version);
                boolean full = in.readBoolean();
                replicas.add(new Replica(endpoint, new Range(left, right), full));
            }
            int cfCount = in.readInt();
            List<String> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, replicas, columnFamilies);
        }

        public long serializedSize(StreamRequest request, int version)
        {
            int size = TypeSizes.sizeof(request.keyspace);
            size += TypeSizes.sizeof(request.replicas.size());
            for (Replica replica : request.replicas)
            {
                size += CompactEndpointSerializationHelper.streamingInstance.serializedSize(replica.getEndpoint(), version);
                size += Token.serializer.serializedSize(replica.getRange().left, version);
                size += Token.serializer.serializedSize(replica.getRange().right, version);
                size += TypeSizes.sizeof(replica.isFull());
            }
            size += TypeSizes.sizeof(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                size += TypeSizes.sizeof(cf);
            return size;
        }
    }
}
