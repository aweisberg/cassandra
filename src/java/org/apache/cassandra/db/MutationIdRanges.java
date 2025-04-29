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
package org.apache.cassandra.db;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.utils.CollectionSerializer;
import org.apache.cassandra.utils.Int64Serializer;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.cassandra.db.memtable.AbstractMemtable.MutationIdCollector;

/**
 * Max mutation ID present in this SSTable for each coordinator log, to determine whether an SSTable is reconciled or
 * not. Once max mutation IDs are reconciled, next compaction can safely mark this SSTabled as repaired. Note that peers
 * may have reconciled all mutations included in an SSTable, but {@link StatsMetadata#repairedAt} is dependent on
 * compaction timing, so "nodetool repair --validate" may report temporary disagreements on the repaired set.
 * <p>
 * This is immutable, so update-heavy paths are expected to use {@link MutationIdCollector}.
 */
public class MutationIdRanges
{
    public static final MutationIdRanges NONE = new MutationIdRanges();

    // Keyed by CoordinatorLogId.
    // A replica can only receive writes from another replica it shares ranges with, and tracked writes are executed by
    // coordinators, so this should contain up to (2*RF - 1) keys. Iterating across keys should not be expensive, but
    // this could benefit from a more compact representation since it's updated on every write.
    @VisibleForTesting
    final Long2ObjectHashMap<MutationId> ids;

    private MutationIdRanges()
    {
        this.ids = new Long2ObjectHashMap<>();
    }

    private MutationIdRanges(Long2ObjectHashMap<MutationId> ids)
    {
        this.ids = ids;
    }

    @Override
    public String toString()
    {
        return "MutationIdRanges{" +
               "ids=" + ids +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        MutationIdRanges that = (MutationIdRanges) o;
        return Objects.equals(ids, that.ids);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ids);
    }

    public MutationIdRanges merge(MutationIdRanges that)
    {
        if (this == NONE)
            return that;
        if (that == NONE)
            return this;
        Long2ObjectHashMap<MutationId> newIds = new Long2ObjectHashMap<>(ids);
        that.ids.forEachLong((logId, right) -> {
            MutationId left = newIds.get(logId);
            if (left == null)
                newIds.put(logId, right);
            else if (ShortMutationId.comparator.compare(left, right) < 0)
                newIds.put(logId, right);
        });

        return new MutationIdRanges(newIds);
    }

    public MutationIdRanges add(MutationId mutationId)
    {
        // Will this allocation will be elided on the path where no update happens?
        Long2ObjectHashMap<MutationId> newIds = new Long2ObjectHashMap<>(ids);
        long logId = mutationId.logId();
        MutationId existing = ids.get(logId);
        if (existing == null)
            newIds.put(logId, mutationId);
        else if (ShortMutationId.comparator.compare(existing, mutationId) < 0)
            newIds.put(logId, mutationId);
        else
            return this;
        return new MutationIdRanges(newIds);
    }

    public int maxOffset(long logId)
    {
        MutationId id = ids.get(logId);
        if (id == null)
            return MutationId.none().offset();
        return id.offset();
    }

    public static final IVersionedSerializer<MutationIdRanges> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(MutationIdRanges metadata, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return;
            CollectionSerializer.serializeMap(Int64Serializer.serializer, MutationId.serializer, metadata.ids, out, version);
        }

        @Override
        public MutationIdRanges deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return MutationIdRanges.NONE;
            IntFunction<Long2ObjectHashMap<MutationId>> map = size -> new Long2ObjectHashMap<>(size, 0.9f);
            Long2ObjectHashMap<MutationId> ids = CollectionSerializer.deserializeMap(Int64Serializer.serializer, MutationId.serializer, map, in, version);
            return new MutationIdRanges(ids);
        }

        @Override
        public long serializedSize(MutationIdRanges metadata, int version)
        {
            if (version < MessagingService.VERSION_52)
                return 0;
            long size = 0;
            size += TypeSizes.INT_SIZE;
            for (Map.Entry<Long, MutationId> entry : metadata.ids.entrySet())
            {
                size += TypeSizes.LONG_SIZE;
                size += MutationId.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };
}
