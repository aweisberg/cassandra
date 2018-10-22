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

package org.apache.cassandra.utils.memory;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.util.Memory;

public class MemoryIntegerSequence implements IntegerSequence
{
    private final Memory integers;
    private final long count;

    public MemoryIntegerSequence(Memory memory, long count)
    {
        integers = memory;
        this.count = count;
    }

    public long count()
    {
        return count;
    }

    public long size()
    {
        return integers.size();
    }

    public long get(long index)
    {
        Preconditions.checkArgument(index >= 0);
        if (index > count)
        {
            return -1;
        }
        return integers.getLong(index * 8);
    }

    public long getMemoized(long index, long lastIndex, long lastValue)
    {
        return get(index);
    }

    public void close()
    {
        integers.close();
    }

    public Memory memory()
    {
        return integers;
    }

    public static IntegerSequence.Builder builder(long count)
    {
        return new Builder(count);
    }

    private static class Builder implements IntegerSequence.Builder
    {
        private Memory memory;
        private final long count;
        private long added = 0;
        public Builder(long count)
        {
             memory = Memory.allocate(count * 8);
             this.count = count;
        }

        public IntegerSequence.Builder add(long value)
        {
            assert added == 0 || memory.getLong((added - 1) * 8 ) < value : String.format("Added %d, Previously added value %d, value %d", added, added == 0 ? -1 : memory.getLong((added - 1) * 8 ), value);
            assert value >= 0 : String.format("Value %d", value);
            memory.setLong(added * 8, value);
            added++;
            return this;
        }

        public IntegerSequence build()
        {
            if (added != count)
            {
                throw new AssertionError(String.format("Count %d and added %d don't match", count, added));
            }
            IntegerSequence is = new MemoryIntegerSequence(memory, count);
            memory = null;
            return is;
        }

        public void close()
        {
            if (memory != null)
                memory.close();
        }
    }
}
