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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;

public class TxnNamedRead extends AbstractSerialized<ReadCommand>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnNamedRead.class);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnNamedRead(null, null, null));

    private final TxnDataName name;
    private final PartitionKey key;

    public TxnNamedRead(TxnDataName name, SinglePartitionReadCommand value)
    {
        super(value);
        this.name = name;
        this.key = new PartitionKey(value.metadata().id, value.partitionKey());
    }

    private TxnNamedRead(TxnDataName name, PartitionKey key, ByteBuffer bytes)
    {
        super(bytes);
        this.name = name;
        this.key = key;
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + name.estimatedSizeOnHeap() + key.estimatedSizeOnHeap() + ByteBufferUtil.estimatedSizeOnHeap(bytes());
    }

    @Override
    protected IVersionedSerializer<ReadCommand> serializer()
    {
        return SinglePartitionReadCommand.serializer;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TxnNamedRead namedRead = (TxnNamedRead) o;
        return name.equals(namedRead.name) && key.equals(namedRead.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), name, key);
    }

    @Override
    public String toString()
    {
        return "TxnNamedRead{name='" + name + '\'' + ", key=" + key + ", update=" + get() + '}';
    }

    public TxnDataName txnDataName()
    {
        return name;
    }

    public PartitionKey key()
    {
        return key;
    }

    public AsyncChain<Data> read(ConsistencyLevel consistencyLevel, Timestamp executeAt)
    {
        SinglePartitionReadCommand command = (SinglePartitionReadCommand) get();
        // TODO (required, safety): before release, double check reasoning that this is safe
//        AccordCommandsForKey cfk = ((SafeAccordCommandStore)safeStore).commandsForKey(key);
//        int nowInSeconds = cfk.nowInSecondsFor(executeAt, isForWriteTxn);
        // It's fine for our nowInSeconds to lag slightly our insertion timestamp, as to the user
        // this simply looks like the transaction witnessed TTL'd data and the data then expired
        // immediately after the transaction executed, and this simplifies things a great deal
        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(executeAt.hlc());
        if (consistencyLevel == null || consistencyLevel == ConsistencyLevel.ONE)
            command = command.withoutReconciliation();
        return performLocalRead(command, nowInSeconds);
    }

    public ReadCommand command()
    {
        return get();
    }

    private AsyncChain<Data> performLocalRead(SinglePartitionReadCommand command, int nowInSeconds)
    {
        Callable<Data> readCallable = () ->
        {
            SinglePartitionReadCommand read = command.withNowInSec(nowInSeconds);

            try (ReadExecutionController controller = read.executionController();
                 PartitionIterator iterator = UnfilteredPartitionIterators.filter(read.executeLocally(controller), read.nowInSec()))
            {
                TxnData result = new TxnData();
                if (iterator.hasNext())
                {
                    TxnDataKeyValue value = new TxnDataKeyValue(iterator.next());
                    if (value.hasRows() || read.selectsFullPartition())
                        result.put(name, value);
                }
                return result;
            }
        };

        return AsyncChains.ofCallable(Stage.READ.executor(), readCallable, (callable, receiver) ->
            new DebuggableTask.RunnableDebuggableTask()
            {
                private final long approxCreationTimeNanos = MonotonicClock.Global.approxTime.now();
                private volatile long approxStartTimeNanos;

                @Override
                public void run()
                {
                    approxStartTimeNanos = MonotonicClock.Global.approxTime.now();

                    try
                    {
                        Data call = callable.call();
                        receiver.accept(call, null);
                    }
                    catch (Throwable t)
                    {
                        logger.debug("AsyncChain Callable threw an Exception", t);
                        receiver.accept(null, t);
                    }
                }

                @Override
                public long creationTimeNanos()
                {
                    return approxCreationTimeNanos;
                }

                @Override
                public long startTimeNanos()
                {
                    return approxStartTimeNanos;
                }

                @Override
                public String description()
                {
                    return command.toCQLString();
                }
            }
        );
    }

    static final IVersionedSerializer<TxnNamedRead> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnNamedRead read, DataOutputPlus out, int version) throws IOException
        {
            TxnDataName.serializer.serialize(read.name, out, version);
            PartitionKey.serializer.serialize(read.key, out, version);
            writeWithVIntLength(read.bytes(), out);
        }

        @Override
        public TxnNamedRead deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnDataName name = TxnDataName.serializer.deserialize(in, version);
            PartitionKey key = PartitionKey.serializer.deserialize(in, version);
            ByteBuffer bytes = readWithVIntLength(in);
            return new TxnNamedRead(name, key, bytes);
        }

        @Override
        public long serializedSize(TxnNamedRead read, int version)
        {
            long size = 0;
            size += TxnDataName.serializer.serializedSize(read.name, version);
            size += PartitionKey.serializer.serializedSize(read.key, version);
            size += serializedSizeWithVIntLength(read.bytes());
            return size;
        }
    };
}
