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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Max mutation ID present in this SSTable for each coordinator log, to determine whether an SSTable is reconciled or
 * not. Once max mutation IDs are reconciled, next compaction can safely mark this SSTabled as repaired. Note that peers
 * may have reconciled all mutations included in an SSTable, but {@link StatsMetadata#repairedAt} is dependent on
 * compaction timing, so "nodetool repair --validate" may report temporary disagreements on the repaired set.
 * <p>
 * A reference to this class should be treated as immutable. Do not cast to {@link MutableCoordinatorLogBoundaries}.
 * Iterable over {@link CoordinatorLogId}.
 */
public abstract class CoordinatorLogBoundaries implements Iterable<Long>
{
    public static class Builder
    {
        private final MutableBuilderHelper helper = new MutableBuilderHelper();

        public CoordinatorLogBoundaries build()
        {
            return helper;
        }

        public void add(MutationId id)
        {
            helper.add(id);
        }

        public void addAll(CoordinatorLogBoundaries from)
        {
            for (long logId : from)
            {
                MutationId max = from.max(logId);
                if (!max.isNone())
                    helper.add(max);
            }
        }

        @Override
        public String toString()
        {
            return "Builder{" +
                   "helper=" + helper +
                   '}';
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public abstract int maxOffset(long logId);
    protected abstract MutationId max(long logId);
    protected abstract int size();

    public static final IVersionedSerializer<CoordinatorLogBoundaries> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(CoordinatorLogBoundaries boundaries, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return;
            out.writeUnsignedVInt32(boundaries.size());
            for (long logId : boundaries)
                MutationId.serializer.serialize(boundaries.max(logId), out, version);
        }

        @Override
        public CoordinatorLogBoundaries deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return CoordinatorLogBoundaries.NONE;
            int size = in.readUnsignedVInt32();
            Builder builder = CoordinatorLogBoundaries.builder();
            for (int i = 0; i < size; i++)
            {
                MutationId mutationId = MutationId.serializer.deserialize(in, version);
                builder.add(mutationId);
            }
            return builder.build();
        }

        @Override
        public long serializedSize(CoordinatorLogBoundaries boundaries, int version)
        {
            if (version < MessagingService.VERSION_52)
                return 0;
            long size = 0;
            size += VIntCoding.computeUnsignedVIntSize(boundaries.size());
            for (long logId : boundaries)
                size += MutationId.serializer.serializedSize(boundaries.max(logId), version);
            return size;
        }
    };

    public static final CoordinatorLogBoundaries NONE = new CoordinatorLogBoundaries()
    {
        @Override
        public int maxOffset(long logId)
        {
            return MutationId.none().offset();
        }

        @Override
        protected MutationId max(long logId)
        {
            return MutationId.none();
        }

        @Override
        protected int size()
        {
            return 0;
        }

        @Override
        public Iterator<Long> iterator()
        {
            return new Iterator<>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public Long next()
                {
                    throw new NoSuchElementException();
                }
            };
        }
    };

    @NotThreadSafe
    private static class MutableBuilderHelper extends CoordinatorLogBoundaries
    {
        private final Long2ObjectHashMap<MutationId> ids = new Long2ObjectHashMap<>();

        public void add(MutationId mutationId)
        {
            long logId = mutationId.logId();
            MutationId existing = ids.get(logId);
            if (existing == null)
                ids.put(logId, mutationId);
            else if (ShortMutationId.comparator.compare(existing, mutationId) < 0)
                ids.put(logId, mutationId);
        }

        @Override
        public int maxOffset(long logId)
        {
            MutationId id = ids.get(logId);
            return id == null ? MutationId.none().offset() : id.offset();
        }

        @Override
        protected MutationId max(long logId)
        {
            return ids.getOrDefault(logId, MutationId.none());
        }

        @Override
        protected int size()
        {
            return ids.size();
        }

        @Override
        public Iterator<Long> iterator()
        {
            return new Iterator<>()
            {
                final Iterator<Long> iterator = ids.keySet().iterator();

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public Long next()
                {
                    return iterator.next();
                }
            };
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            MutableBuilderHelper longs = (MutableBuilderHelper) o;
            return Objects.equals(ids, longs.ids);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(ids);
        }

        @Override
        public String toString()
        {
            return "MutableBuilderHelper{" +
                   "ids=" + ids +
                   '}';
        }
    }
}
