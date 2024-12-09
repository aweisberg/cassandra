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

package org.apache.cassandra.service.accord.interop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables.Slice;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers.ReadDataSerializer;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.Pair;

import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class AccordInteropRead extends ReadData
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteropRead.class);

    public static final IVersionedSerializer<AccordInteropRead> requestSerializer = new ReadDataSerializer<AccordInteropRead>()
    {
        @Override
        public void serialize(AccordInteropRead read, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out, version);
            KeySerializers.participants.serialize(read.readScope, out, version);
            out.writeUnsignedVInt(read.executeAtEpoch);
            out.writeUnsignedVInt32(read.txnReadName);
        }

        @Override
        public AccordInteropRead deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> readScope = KeySerializers.participants.deserialize(in, version);
            long executeAtEpoch = in.readUnsignedVInt();
            int txnReadName = in.readUnsignedVInt32();
            return new AccordInteropRead(txnId, readScope, executeAtEpoch, txnReadName);
        }

        @Override
        public long serializedSize(AccordInteropRead read, int version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId, version)
                   + KeySerializers.participants.serializedSize(read.readScope, version)
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch)
                   + TypeSizes.sizeofUnsignedVInt(read.txnReadName);
        }
    };

    public static final IVersionedSerializer<ReadReply> replySerializer = new ReadDataSerializers.ReplySerializer<>(LocalReadData.serializer);

    private static class LocalReadData implements Data
    {
        private static final IVersionedSerializer<Pair<AccordRoutingKey, ReadResponse>> responseSerializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Pair<AccordRoutingKey, ReadResponse> t, DataOutputPlus out, int version) throws IOException
            {
                serializeNullable(t.left, out, version, AccordRoutingKey.serializer);
                ReadResponse.serializer.serialize(t.right, out, version);
            }

            @Override
            public Pair<AccordRoutingKey, ReadResponse> deserialize(DataInputPlus in, int version) throws IOException
            {
                return Pair.create(deserializeNullable(in, version, AccordRoutingKey.serializer),
                                   ReadResponse.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(Pair<AccordRoutingKey, ReadResponse> t, int version)
            {
                return serializedNullableSize(t.left, version, AccordRoutingKey.serializer)
                       + ReadResponse.serializer.serializedSize(t.right, version);
            }
        };

        static final IVersionedSerializer<LocalReadData> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(LocalReadData data, DataOutputPlus out, int version) throws IOException
            {
                out.writeBoolean(data.isRangeResponse);
                serializeCollection(data.responses, out, version, responseSerializer);
           }

            @Override
            public LocalReadData deserialize(DataInputPlus in, int version) throws IOException
            {
                boolean isRangeResponse = in.readBoolean();
                List<Pair<AccordRoutingKey, ReadResponse>> responses = deserializeList(in, version, responseSerializer);
                return new LocalReadData(responses, isRangeResponse, version);
            }

            @Override
            public long serializedSize(LocalReadData data, int version)
            {
                return TypeSizes.BOOL_SIZE
                       + serializedCollectionSize(data.responses, version, responseSerializer);
            }
        };

        List<Pair<AccordRoutingKey, ReadResponse>> responses;
        private final int version;
        final boolean isRangeResponse;

        public LocalReadData(@Nullable AccordRoutingKey start, @Nonnull ReadResponse response, boolean isRangeResponse)
        {
            checkNotNull(response, "response is null");
            responses = ImmutableList.of(Pair.create(start, response));
            this.isRangeResponse = isRangeResponse;
            version = -1;
        }

        public LocalReadData(@Nonnull List<Pair<AccordRoutingKey, ReadResponse>> responses, boolean isRangeResponse, int version)
        {
            checkNotNull(responses);
            checkArgument(!responses.isEmpty(), "responses should not be empty");
            checkState(responses.size() == 1 || isRangeResponse, "Should only have multiple responses with a range response");
            this.responses = responses;
            this.isRangeResponse = isRangeResponse;
            this.version = version;
        }

        @Override
        public String toString()
        {
            return "LocalReadData{" + responses + '}';
        }

        @Override
        public Data merge(Data data)
        {
            checkState(isRangeResponse, "Should only ever be a single partition");
            LocalReadData other = (LocalReadData)data;
            checkState(other.isRangeResponse, "Other should also be a range response");
            if (responses.size() == 1)
            {
                List<Pair<AccordRoutingKey, ReadResponse>> merged = new ArrayList<>();
                merged.add(responses.get(0));
                responses = merged;
            }
            responses.addAll(other.responses);
            return this;
        }
    }

    static class ReadCallback extends AccordInteropReadCallback<ReadResponse>
    {
        public ReadCallback(Node.Id id, InetAddressAndPort endpoint, Message<?> message, RequestCallback<ReadResponse> wrapped, MaximalCommitSender maximalCommitSender)
        {
            super(id, endpoint, message, wrapped, maximalCommitSender);
        }

        @Override
        ReadResponse convertResponse(ReadOk ok)
        {
            LocalReadData localReadData = ((LocalReadData)ok.data);
            // Range reads will be spread across command stores and need to be merged in token order
            List<Pair<AccordRoutingKey, ReadResponse>> responses = localReadData.responses;
            if (responses.size() == 1)
                return responses.get(0).right;
            responses = new ArrayList(responses);
            Collections.sort(responses, Comparator.comparing(Pair::left));
            return ReadResponse.merge(Lists.transform(responses, Pair::right), localReadData.version);
        }
    }

    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(ReadyToExecute, PreApplied);

    private final int txnReadName;

    public AccordInteropRead(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, long executeAtEpoch, int txnReadName)
    {
        super(to, topologies, txnId, readScope, executeAtEpoch);
        this.txnReadName = txnReadName;
    }

    public AccordInteropRead(TxnId txnId, Participants<?> readScope, long executeAtEpoch, int txnReadName)
    {
        super(txnId, readScope, executeAtEpoch);
        this.txnReadName = txnReadName;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.readTxnData;
    }

    @Override
    protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Ranges unavailable)
    {
        TxnRead txnRead = (TxnRead)txn.read();
        for (TxnNamedRead txnNamedRead : txnRead)
        {
            if (txnNamedRead.txnDataName() == txnReadName)
            {
                checkState(unavailable.isEmpty(), "Eventually consistent read coordinators can't handle unavailable ranges");
                Ranges ranges = safeStore.ranges().allAt(executeAt).without(unavailable).intersecting(readScope, Slice.Minimal);
                long nowInSeconds = TxnNamedRead.nowInSeconds(executeAt);
                List<AsyncChain<Data>> chains = new ArrayList<>(ranges.size());
                for (Range r : ranges)
                {
                    ReadCommand readCommand = txnNamedRead.command();
                    AccordRoutingKey routingKey = null;
                    if (readCommand.isRangeRequest())
                    {
                        readCommand = txnNamedRead.commandForSubrange((PartitionRangeReadCommand) readCommand, r, txnRead.cassandraConsistencyLevel(), nowInSeconds);
                        routingKey = ((TokenRange)r).start();
                    }
                    else
                    {
                        readCommand = ((SinglePartitionReadCommand)readCommand).withTransactionalSettings(txnNamedRead.readsWithoutReconciliation(txnRead.cassandraConsistencyLevel()), nowInSeconds);
                    }
                    ReadCommand readCommandFinal = readCommand;
                    AccordRoutingKey routingKeyFinal = routingKey;
                    logger.info("Ariel submitting read for routing key " + routingKeyFinal);
                    chains.add(AsyncChains.ofCallable(Stage.READ.executor(), () -> new LocalReadData(routingKeyFinal, ReadCommandVerbHandler.instance.doRead(readCommandFinal, false), readCommandFinal.isRangeRequest())));
                }

                if (chains.isEmpty())
                    return AsyncChains.success(null);

                return AsyncChains.reduce(chains, Data::merge);
            }
        }
        throw new IllegalStateException("Didn't find a matching read with txnReadName " + txnRead + " at " + safeStore + " for txn with executeAt " + executeAt);
        // TODO (required): subtract unavailable ranges, either from read or from response (or on coordinator)
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    protected ReadOk constructReadOk(Ranges unavailable, Data data)
    {
        return new InteropReadOk(unavailable, data);
    }

    @Override
    public MessageType type()
    {
        return AccordMessageType.INTEROP_READ_REQ;
    }

    @Override
    public String toString()
    {
        return "AccordInteropRead{" +
               "txnId=" + txnId +
               "txnReadName=" + txnReadName +
               '}';
    }

    private static class InteropReadOk extends ReadOk
    {
        public InteropReadOk(@Nullable Ranges unavailable, @Nullable Data data)
        {
            super(unavailable, data);
        }

        @Override
        public MessageType type()
        {
            return AccordMessageType.INTEROP_READ_RSP;
        }
    }
}
