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
import java.util.Comparator;
import java.util.RandomAccess;
import javax.annotation.Nullable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static com.google.common.base.Preconditions.checkState;

/*
 * Important to note that these aren't sorted so you will definitely want to sort first
 */
public class TxnDataRangeValue extends ArrayList<FilteredPartition> implements TxnDataValue, RandomAccess
{

    public TxnDataRangeValue() {}

    public TxnDataRangeValue(int size)
    {
        super(size);
    }

    @Override
    public Kind kind()
    {
        return Kind.range;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        // TODO (required): Account for the rest of the memory
        long size = 0;
        for (FilteredPartition partition : this)
        {
            for (Row row : partition)
                size += row.unsharedHeapSize();
        }
        return size;
    }

    @Override
    public TxnDataRangeValue merge(TxnDataValue other)
    {
        // TODO (nicetohave): This discards the sorted property
        if (isEmpty())
            return (TxnDataRangeValue)other;

        TxnDataRangeValue otherRange = (TxnDataRangeValue)other;
        if (otherRange.isEmpty())
            return this;

        TableId tableId = tableId();
        for (FilteredPartition partition : otherRange)
            checkState(partition.metadata().id.equals(tableId), "All values should be for the same table");

        addAll(((TxnDataRangeValue)other));
        return this;
    }

    public void sort()
    {
        sort(Comparator.comparing(FilteredPartition::partitionKey));
    }

    private @Nullable TableId tableId()
    {
        if (isEmpty())
            return null;
        return get(0).metadata().id;
    }

    public static final TxnDataValueSerializer<TxnDataRangeValue> serializer = new TxnDataValueSerializer<TxnDataRangeValue>()
    {
        @Override
        public void serialize(TxnDataRangeValue value, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(value.size());

            if (value.isEmpty())
                return;

            TableId.serializer.serialize(value.tableId(), out, version);
            for (FilteredPartition partition : value)
            {
                try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                {
                    UnfilteredRowIteratorSerializer.serializer.serialize(iterator, ColumnFilter.all(partition.metadata()), out, version, partition.rowCount());
                }
            }
        }

        @Override
        public TxnDataRangeValue deserialize(DataInputPlus in, int version) throws IOException
        {
            int numPartitions = in.readUnsignedVInt32();
            TxnDataRangeValue value = new TxnDataRangeValue(numPartitions);
            if (numPartitions == 0)
                return value;
            // TODO (required): This needs to be updated for schema change to use the correct cluster metadata
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            for (int i = 0; i < numPartitions; i++)
            {
                try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, ColumnFilter.all(metadata), DeserializationHelper.Flag.FROM_REMOTE))
                {
                    value.add(new FilteredPartition(UnfilteredRowIterators.filter(partition, 0)));
                }
            }
            return value;
        }

        @Override
        public long serializedSize(TxnDataRangeValue value, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(value.size());
            if (value.size() == 0)
                return size;
            size += TableId.serializer.serializedSize(value.tableId(), version);
            for (FilteredPartition partition : value)
            {
                try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                {
                    size += UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, ColumnFilter.all(partition.metadata()), version, partition.rowCount());
                }
            }
            return size;
        }
    };
}
