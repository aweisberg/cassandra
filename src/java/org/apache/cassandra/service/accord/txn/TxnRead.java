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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Read;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.service.accord.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.service.accord.IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.CAS_READ;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.USER;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class TxnRead extends AbstractKeySorted<TxnNamedRead> implements Read
{
    public static final TxnRead EMPTY = new TxnRead(new TxnNamedRead[0], null);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY);
    private static final Comparator<TxnNamedRead> TXN_NAMED_READ_KEY_COMPARATOR = Comparator.comparing(a -> ((PartitionKey) a.keys().get(0)));
    private static final Comparator<TxnNamedRead> TXN_NAMED_READ_RANGE_COMPARATOR = Comparator.comparing(a -> ((TokenRange) a.keys()).start());

    // Cassandra's consistency level used by Accord to safely read data written outside of Accord
    @Nullable
    private final ConsistencyLevel cassandraConsistencyLevel;

    private TxnRead(@Nonnull TxnNamedRead[] items, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        checkNotNull(items, "items is null");
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    private TxnRead(@Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel cassandraConsistencyLevel)
    {
        super(items);
        checkNotNull(items, "items is null");
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
    }

    private static void sortReads(List<TxnNamedRead> reads)
    {
        if (reads.size() == 0)
            return;
        Domain domain = reads.get(0).keys().domain();
        switch (domain)
        {
            case Key:
                reads.sort(TXN_NAMED_READ_KEY_COMPARATOR);
                break;
            case Range:
                reads.sort(TXN_NAMED_READ_RANGE_COMPARATOR);
                break;
            default:
                throw new IllegalStateException("Unhandled domain " + domain);
        }
    }

    public static TxnRead createTxnRead(@Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel consistencyLevel)
    {
        sortReads(items);
        return new TxnRead(items, consistencyLevel);
    }

    public static TxnRead createSerialRead(List<SinglePartitionReadCommand> readCommands, ConsistencyLevel consistencyLevel)
    {
        List<TxnNamedRead> reads = new ArrayList<>(readCommands.size());
        for (int i = 0; i < readCommands.size(); i++)
            reads.add(new TxnNamedRead(txnDataName(USER, i), readCommands.get(i)));
        sortReads(reads);
        return new TxnRead(reads, consistencyLevel);
    }

    public static TxnRead createCasRead(SinglePartitionReadCommand readCommand, ConsistencyLevel consistencyLevel)
    {
        TxnNamedRead read = new TxnNamedRead(txnDataName(CAS_READ), readCommand);
        return new TxnRead(ImmutableList.of(read), consistencyLevel);
    }

    // A read that declares it will read from keys but doesn't actually read any data so dependent transactions will
    // still be applied first
    public static TxnRead createNoOpRead(Keys keys)
    {
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++)
            reads.add(new TxnNamedRead(txnDataName(USER, i), Keys.of(keys.get(i)), null));
        return new TxnRead(reads, null);
    }

    public static TxnRead createRangeRead(PartitionRangeReadCommand command, List<AbstractBounds<PartitionPosition>> ranges, ConsistencyLevel consistencyLevel)
    {
        return new TxnRead(ImmutableList.of(new TxnNamedRead(txnDataName(USER), ranges, command)), consistencyLevel);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (TxnNamedRead read : items)
            size += read.estimatedSizeOnHeap();
        return size;
    }

    @Override
    int compareNonKeyFields(TxnNamedRead left, TxnNamedRead right)
    {
        return Integer.compare(left.txnDataName(), right.txnDataName());
    }

    @Override
    Seekables<?, ?> getKeys(TxnNamedRead read)
    {
        return read.keys();
    }

    @Override
    TxnNamedRead[] newArray(int size)
    {
        return new TxnNamedRead[size];
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return itemKeys;
    }

    public ConsistencyLevel cassandraConsistencyLevel()
    {
        return cassandraConsistencyLevel;
    }

    @Override
    public Read slice(Ranges ranges)
    {
        return intersecting(itemKeys.slice(ranges));
    }

    @Override
    public Read intersecting(Participants<?> participants)
    {
        return intersecting(itemKeys.intersecting(participants));
    }

    private Read intersecting(Seekables<?, ?> select)
    {
        // TODO (review): Why construct this keys at all and not just check against select?
        Seekables<?, ?> keys = (Seekables<?, ?>)itemKeys.intersecting(select);
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());

        switch (keys.domain())
        {
            case Key:
                for (TxnNamedRead read : items)
                    if (keys.intersects(read.keys()))
                        reads.add(read);
                break;
            case Range:
                for (TxnNamedRead read : items)
                    if (keys.intersects((Ranges)read.keys()))
                        reads.add(read);
                break;
            default:
                throw new IllegalStateException("Unhandled domain " + keys.domain());
        }

        return createTxnRead(reads, cassandraConsistencyLevel);
    }

    @Override
    public Read merge(Read read)
    {
        List<TxnNamedRead> reads = new ArrayList<>(items.length);
        Collections.addAll(reads, items);

        for (TxnNamedRead namedRead : (TxnRead) read)
            if (!reads.contains(namedRead))
                reads.add(namedRead);

        return createTxnRead(reads, cassandraConsistencyLevel);
    }

    public void unmemoize()
    {
        for (TxnNamedRead read : items)
            read.unmemoize();
    }

    @Override
    public AsyncChain<Data> read(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        // Set to null since we don't need it and interop can pass in null
        safeStore = null;

        List<AsyncChain<Data>> results = new ArrayList<>();
        forEachWithKey(key, read -> results.add(read.read(cassandraConsistencyLevel, key, executeAt)));

        if (results.isEmpty())
            // Result type must match everywhere
            return AsyncChains.success(new TxnData());

        if (results.size() == 1)
            return results.get(0);

        return AsyncChains.reduce(results, Data::merge);
    }

    public static final IVersionedSerializer<TxnRead> serializer = new IVersionedSerializer<TxnRead>()
    {
        @Override
        public void serialize(TxnRead read, DataOutputPlus out, int version) throws IOException
        {
            serializeArray(read.items, out, version, TxnNamedRead.serializer);
            serializeNullable(read.cassandraConsistencyLevel, out, version, consistencyLevelSerializer);
        }

        @Override
        public TxnRead deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnNamedRead[] items = deserializeArray(in, version, TxnNamedRead.serializer, TxnNamedRead[]::new);
            ConsistencyLevel consistencyLevel = deserializeNullable(in, version, consistencyLevelSerializer);
            return new TxnRead(items, consistencyLevel);
        }

        @Override
        public long serializedSize(TxnRead read, int version)
        {
            long size = 0;
            size += serializedArraySize(read.items, version, TxnNamedRead.serializer);
            size += serializedNullableSize(read.cassandraConsistencyLevel, version, consistencyLevelSerializer);
            return size;
        }
    };
}
