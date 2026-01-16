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
package org.apache.cassandra.io.util;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

public class DirectThreadLocalByteBufferHolderTest
{

    @Test
    public void testGetBuffer()
    {
        int blockSize = 4096;
        int alignedBufferSize = blockSize * 4;

        try (DirectThreadLocalByteBufferHolder holder = new DirectThreadLocalByteBufferHolder(blockSize))
        {
            // Initial buffer creation
            ByteBuffer byteBuffer = holder.getBuffer(alignedBufferSize);

            Assert.assertEquals(alignedBufferSize, byteBuffer.limit());
            Assert.assertEquals(0, byteBuffer.position());
            Assert.assertEquals(byteBuffer, holder.local.get());
            byteBuffer.put(new byte[alignedBufferSize]);

            // Re-use buffer of same size
            byteBuffer = holder.getBuffer(alignedBufferSize);
            Assert.assertEquals(alignedBufferSize, byteBuffer.limit());
            Assert.assertEquals(0, byteBuffer.position());
            byteBuffer.put(new byte[alignedBufferSize]);

            // Get buffer of a different, greater, non-aligned size
            alignedBufferSize += alignedBufferSize + blockSize;
            int nonAlignedBufferSize = alignedBufferSize - (blockSize / 2);

            ByteBuffer oldBuffer = byteBuffer;
            byteBuffer = holder.getBuffer(nonAlignedBufferSize);
            Assert.assertEquals(alignedBufferSize, byteBuffer.limit());
            Assert.assertEquals(0, byteBuffer.position());
            Assert.assertNotEquals(oldBuffer, holder.local.get());
            Assert.assertEquals(byteBuffer, holder.local.get());
        }
    }

    @Test
    public void testClose()
    {
        int blockSize = 4096;
        int bufferSize = blockSize * 2;

        DirectThreadLocalByteBufferHolder holder = new DirectThreadLocalByteBufferHolder(blockSize);

        // Allocate a buffer
        ByteBuffer buffer = holder.getBuffer(bufferSize);
        Assert.assertNotNull(buffer);
        Assert.assertEquals(bufferSize, buffer.limit());
        Assert.assertNotNull(holder.local.getIfExists());

        // Close should clean up and remove the ThreadLocal
        holder.close();
        Assert.assertNull("ThreadLocal should be removed after close", holder.local.getIfExists());

        // Multiple close() calls should be safe
        holder.close();
        Assert.assertNull(holder.local.getIfExists());
    }

    @Test
    public void testCloseWithoutAllocation()
    {
        DirectThreadLocalByteBufferHolder holder = new DirectThreadLocalByteBufferHolder(4096);

        // Close without ever allocating should be safe
        holder.close();
        Assert.assertNull("ThreadLocal should not exist if never used", holder.local.getIfExists());
    }

    @Test
    public void testCloseFreesNativeMemory() throws Exception
    {
        int blockSize = 4096;
        // Use a large buffer size to make the memory change measurable
        int bufferSize = 1024 * 1024; // 1MB
        int numThreads = 4;

        BufferPoolMXBean directPool = getDirectBufferPool();

        long memoryBefore = directPool.getMemoryUsed();

        DirectThreadLocalByteBufferHolder holder = new DirectThreadLocalByteBufferHolder(blockSize);
        CountDownLatch allAllocated = new CountDownLatch(numThreads);
        CountDownLatch canExit = new CountDownLatch(1);

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++)
        {
            threads[i] = new Thread(() -> {
                holder.getBuffer(bufferSize);
                allAllocated.countDown();
                try
                {
                    canExit.await();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            });
            threads[i].start();
        }

        allAllocated.await();

        long memoryAfterAlloc = directPool.getMemoryUsed();
        // Each aligned buffer is bufferSize + blockSize (for alignment padding)
        long expectedIncrease = (long) numThreads * (bufferSize + blockSize);
        Assert.assertTrue("Memory should increase by ~" + expectedIncrease + " after allocation " +
                          "(before=" + memoryBefore + ", after=" + memoryAfterAlloc + ")",
                          memoryAfterAlloc >= memoryBefore + expectedIncrease * 0.9);

        holder.close();

        long memoryAfterClose = directPool.getMemoryUsed();
        Assert.assertTrue("Memory should decrease after close (before=" + memoryAfterAlloc +
                          ", after=" + memoryAfterClose + ", expected decrease ~" + expectedIncrease + ")",
                          memoryAfterClose <= memoryAfterAlloc - expectedIncrease * 0.9);

        canExit.countDown();
        for (Thread t : threads)
            t.join();
    }

    private static BufferPoolMXBean getDirectBufferPool()
    {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools)
        {
            if (pool.getName().equals("direct"))
                return pool;
        }
        throw new IllegalStateException("Direct buffer pool not found");
    }

}