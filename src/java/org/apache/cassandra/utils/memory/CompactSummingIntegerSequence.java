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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.Memory;

/**
 * A sequence of increasing integers stored in a more compact representation where every 61st integer is stored as 8-bytes and each
 * subsequent integer is stored as a 2-byte offset from the most recent value. This requires 26.6% of the space of storing
 * 8-byte values.
 */
public class CompactSummingIntegerSequence implements IntegerSequence
{
    public static final int CHUNK_SIZE_IN_BYTES = 128;
    public static final int FULL_OFFSET_SIZE_IN_BYTES = 8;
    public static final int OFFSET_SIZE_IN_BYTES = 2;
    //Number off offsets per chunk including a full offset, ends up being 40 partial and one full
    public static final int OFFSETS_PER_CHUNK = (CHUNK_SIZE_IN_BYTES - FULL_OFFSET_SIZE_IN_BYTES) / OFFSET_SIZE_IN_BYTES + 1;

    //This is the maximum safe value to add where we won't overflow the 2-bytes in between each offset
    public static final int MAX_SAFE_SPAN = (1 << 16) - 1;

    private final Memory buffer;
    private final long count;

    //For performance always store the last index and value to reduce repeated work
    private long lastFetchIndex = Long.MIN_VALUE;
    private long lastFetchValue = Long.MIN_VALUE;

    public CompactSummingIntegerSequence(Memory buffer, long count)
    {
        this.buffer = buffer;
        this.count = count;
    }

    @Override
    public void finalize()
    {
        buffer.free();
    }

    public long count()
    {
        return count;
    }

    public long size()
    {
        return buffer.size();
    }

    public long get(long index)
    {
        Preconditions.checkArgument(index >= 0);
        if (index >= count)
            return -1;

        long chunk = index / OFFSETS_PER_CHUNK;
        long offset = index % OFFSETS_PER_CHUNK;

        long fullOffsetPosition = chunk * CHUNK_SIZE_IN_BYTES;
        long fullOffset = buffer.getLong(fullOffsetPosition);
        if (offset == 0)
            return fullOffset;

        long offsetPosition = fullOffsetPosition + FULL_OFFSET_SIZE_IN_BYTES;
        int partialOffset = 0;
        for (int ii = 0; ii < offset; ii++)
        {
            partialOffset += buffer.getShort(offsetPosition) & 0xFFFF;
            offsetPosition += 2;
        }
        return partialOffset + fullOffset;
    }

    public long getMemoized(long index, long lastFetchIndex, long lastFetchValue)
    {
        Preconditions.checkArgument(index >= 0);
        if (index >= count)
            return -1;

        //Can return the same memoized value
        if (index == lastFetchIndex)
            return lastFetchValue;

        long chunk = index / OFFSETS_PER_CHUNK;
        long offset = index % OFFSETS_PER_CHUNK;

        long fullOffsetPosition = chunk * CHUNK_SIZE_IN_BYTES;
        long fullOffset = buffer.getLong(fullOffsetPosition);
        if (offset == 0)
            return fullOffset;

        long offsetPosition = fullOffsetPosition + FULL_OFFSET_SIZE_IN_BYTES;
        //Don't need to repeat the summing work
        if (index - 1 == lastFetchIndex)
        {
            return lastFetchValue + (buffer.getShort(offsetPosition + ((offset - 1) * 2)) & 0xFFFF);
        }
        else
        {
            //Slow path for random access, need to sum
            int partialOffset = 0;
            for (int ii = 0; ii < offset; ii++)
            {
                partialOffset += buffer.getShort(offsetPosition) & 0xFFFF;
                offsetPosition += 2;
            }
            return partialOffset + fullOffset;
        }
    }

    public long getMemoizing(long index)
    {
        Preconditions.checkArgument(index >= 0);
        if (index >= count)
            return -1;

        long value = getMemoized(index, lastFetchIndex, lastFetchValue);
        lastFetchIndex = index;
        lastFetchValue = value;
        return value;
    }

    public void close()
    {
        buffer.free();
    }

    public Memory memory()
    {
        return null;
    }

    public static void main(String args[])
    {
        int num = 65535;
        System.out.printf("%d%n", num);
        short shot = (short)num;
        System.out.printf("%d%n", shot);
        num = (int)shot & 0xffff;
        System.out.printf("%d%n", num);
        try (Builder builder = new Builder(MAX_SAFE_SPAN / 2 + 1))
        {
            List<Long> expected = new ArrayList<>();
            for (int ii = 0; ii <= MAX_SAFE_SPAN / 2; ii++)
            {
                //            builder.add(ii + ii);
                //            expected.add((long)ii + ii);
                builder.add(ii * MAX_SAFE_SPAN);
                expected.add(ii * (long) MAX_SAFE_SPAN);
            }
            //        expected.add(MAX_SAFE_SPAN * 2L);
            //        builder.add(MAX_SAFE_SPAN * 2);
            //        expected.add(MAX_SAFE_SPAN * 3L);
            //        builder.add(MAX_SAFE_SPAN * 3);

            try (CompactSummingIntegerSequence c = builder.build())
            {
                for (int ii = 0; ii < expected.size(); ii++)
                {
                    assert expected.get(ii).equals(c.get(ii)) : String.format("%d is not %d", c.get(ii), expected.get(ii));
                }
            }
        }
    }

    public static Builder builder(long count)
    {
        return new Builder(count);
    }

    private static class Builder implements IntegerSequence.Builder
    {
        private Memory buffer;
        private final long count;
        private long added;
        private long writeOffset = 0;
        private long lastValue = 0;

        public Builder(long count)
        {
            buffer = Memory.allocate(Math.max(CHUNK_SIZE_IN_BYTES, ((count / OFFSETS_PER_CHUNK) + 1) * CHUNK_SIZE_IN_BYTES));
            this.count = count;
        }

        public IntegerSequence.Builder add(long value)
        {
            assert lastValue <= value : String.format("lastValue %d, value %d", lastValue, value);
            assert value >= 0 : String.format("Value %d", value);

            if (writeOffset % CHUNK_SIZE_IN_BYTES == 0)
            {
                buffer.setLong(writeOffset, value);
                writeOffset += 8;
            }
            else
            {
                long partialOffset = value - lastValue;
                assert partialOffset > 0;
                assert partialOffset <= MAX_SAFE_SPAN : String.format("Partial offset %d is not <= MAX_SAFE_SPAN %d, value %d and lastValue %d", partialOffset, MAX_SAFE_SPAN, value, lastValue);
                buffer.setShort(writeOffset, (short)(partialOffset));
                writeOffset += 2;
            }
            lastValue = value;
            added++;
            return this;
        }

        @Override
        public void finalize()
        {
            buffer.free();
            buffer = null;
        }

        public CompactSummingIntegerSequence build()
        {
            if (added != count)
            {
                throw new AssertionError(String.format("Count %d and added %d don't match", count, added));
            }
            CompactSummingIntegerSequence retval = new CompactSummingIntegerSequence(buffer, count);
            buffer = null;
            return retval;
        }

        public void close()
        {
            if (buffer != null)
                buffer.close();
        }
    }
}
