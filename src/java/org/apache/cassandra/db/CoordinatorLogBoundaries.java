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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Immutable. Iterable over {@link CoordinatorLogId}.
 */
public abstract class CoordinatorLogBoundaries implements Iterable<Long>
{
    public static class Builder
    {
        private final MutableCoordinatorLogBoundaries boundaries = new MutableCoordinatorLogBoundaries();

        public CoordinatorLogBoundaries build()
        {
            return boundaries;
        }

        public void add(MutationId id)
        {
            boundaries.add(id);
        }

        public void addAll(CoordinatorLogBoundaries from)
        {
            for (long logId : from)
            {
                MutationId max = from.max(logId);
                if (!max.isNone())
                    boundaries.add(max);
            }
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
            return new Iterator<Long>()
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
}
