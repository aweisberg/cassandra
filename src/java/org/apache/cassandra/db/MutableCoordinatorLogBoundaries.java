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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.ShortMutationId;

/**
 * Thread-safe.
 */
public class MutableCoordinatorLogBoundaries extends CoordinatorLogBoundaries
{
    private static final MutationId NONE = MutationId.none();
    private static final int NONE_OFFSET = NONE.offset();

    private final ConcurrentHashMap<Long, MutationId> ids = new ConcurrentHashMap<>();

    public void add(MutationId mutationId)
    {
        long logId = mutationId.logId();
        ids.merge(logId, mutationId, (existing, updating) -> {
            if (ShortMutationId.comparator.compare(existing, updating) < 0)
                return updating;
            return existing;
        });
    }

    @Override
    public int maxOffset(long logId)
    {
        MutationId id = ids.get(logId);
        return id == null ? NONE_OFFSET : id.offset();
    }

    @Override
    protected MutationId max(long logId)
    {
        return ids.getOrDefault(logId, NONE);
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
        MutableCoordinatorLogBoundaries longs = (MutableCoordinatorLogBoundaries) o;
        return Objects.equals(ids, longs.ids);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ids);
    }
}
