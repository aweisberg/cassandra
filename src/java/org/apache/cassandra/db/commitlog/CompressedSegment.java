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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.SyncUtil;

import com.google.common.util.concurrent.MoreExecutors;

/*
 * Compressed commit log segment. Provides an in-memory buffer for the mutation threads. On sync compresses the written
 * section of the buffer and writes it to the destination channel.
 */
public class CompressedSegment extends CommitLogSegment
{
    private static final ThreadLocal<ByteBuffer> compressedBufferHolder = new ThreadLocal<ByteBuffer>() {
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.allocate(0);
        }
    };

    static final Object bufferPoolLock = new Object();
    static final Queue<ByteBuffer> bufferPool = new ArrayDeque<>();
    static final Queue<CompletableFuture<ByteBuffer>> bufferPoolWaiters = new ArrayDeque<>();
    static int allocatedBuffers = 0;

    /**
     * Maximum number of buffers in the compression pool. The default value is 3, it should not be set lower than that
     * (one segment in compression, one written to, one in reserve); delays in compression may cause the log to use
     * more, depending on how soon the sync policy stops all writing threads.
     */
    static final int MAX_BUFFERPOOL_SIZE = DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool();

    static final int COMPRESSED_MARKER_SIZE = SYNC_MARKER_SIZE + 4;
    final ICompressor compressor;

    volatile long lastWrittenPos = 0;

    /**
     * Constructs a new segment file.
     */
    private CompressedSegment(CommitLog commitLog, ByteBuffer buffer)
    {
        super(commitLog, buffer);
        this.compressor = commitLog.compressor;
        try
        {
            channel.write((ByteBuffer) buffer.duplicate().flip());
            commitLog.allocator.addSize(lastWrittenPos = buffer.position());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    ByteBuffer allocate(int size)
    {
        return compressor.preferredBufferType().allocate(size);
    }

    @Override
    ByteBuffer createBuffer(CommitLog cl)
    {
        throw new UnsupportedOperationException();
    }

    /*
     * Create a listenable future for the next available buffer
     */
    static CompletableFuture<ByteBuffer> createBufferListener(CommitLog commitLog)
    {
        synchronized (bufferPoolLock)
        {
            ByteBuffer buf = bufferPool.poll();
            if (buf == null)
            {
                if (allocatedBuffers >= MAX_BUFFERPOOL_SIZE)
                {
                    //CompleatableFuture constructor is an instrumentation point for byteman in CLSMTest
                    CompletableFuture<ByteBuffer> waiter = new CompletableFuture<>();
                    bufferPoolWaiters.offer(waiter);
                    return waiter;
                }
                else
                {
                    allocatedBuffers++;
                    // this.compressor is not yet set, so we must use the commitLog's one.
                    buf = commitLog.compressor.preferredBufferType().allocate(DatabaseDescriptor.getCommitLogSegmentSize());
                }
            } else
                buf.clear();
            return CompletableFuture.completedFuture(buf);
        }
    }

    static long startMillis = System.currentTimeMillis();

    @Override
    void write(int startMarker, int nextMarker)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        int length = nextMarker - contentStart;
        // The length may be 0 when the segment is being closed.
        assert length > 0 || length == 0 && !isStillAllocating();

        try
        {
            int neededBufferSize = compressor.initialCompressedBufferLength(length) + COMPRESSED_MARKER_SIZE;
            ByteBuffer compressedBuffer = compressedBufferHolder.get();
            if (compressor.preferredBufferType() != BufferType.typeOf(compressedBuffer) ||
                compressedBuffer.capacity() < neededBufferSize)
            {
                FileUtils.clean(compressedBuffer);
                compressedBuffer = allocate(neededBufferSize);
                compressedBufferHolder.set(compressedBuffer);
            }

            ByteBuffer inputBuffer = buffer.duplicate();
            inputBuffer.limit(contentStart + length).position(contentStart);
            compressedBuffer.limit(compressedBuffer.capacity()).position(COMPRESSED_MARKER_SIZE);
            compressor.compress(inputBuffer, compressedBuffer);

            compressedBuffer.flip();
            compressedBuffer.putInt(SYNC_MARKER_SIZE, length);

            // Only one thread can be here at a given time.
            // Protected by synchronization on CommitLogSegment.sync().
            writeSyncMarker(compressedBuffer, 0, (int) channel.position(), (int) channel.position() + compressedBuffer.remaining());
            commitLog.allocator.addSize(compressedBuffer.limit());
            channel.write(compressedBuffer);
            assert channel.position() - lastWrittenPos == compressedBuffer.limit();
            lastWrittenPos = channel.position();
            SyncUtil.force(channel, true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    @Override
    protected void internalClose()
    {
        synchronized (bufferPoolLock)
        {
            if (allocatedBuffers <= MAX_BUFFERPOOL_SIZE)
            {
                if (!bufferPoolWaiters.isEmpty())
                    bufferPoolWaiters.poll().complete(buffer);
                else
                    bufferPool.add(buffer);
            }
            else
                throw new RuntimeException("Shouldn't allocate more than MAX_BUFFERPOOL_SIZE buffers");
        }

        super.internalClose();
    }

    static void shutdown()
    {
        bufferPool.clear();
        bufferPoolWaiters.clear();
        allocatedBuffers = 0;
    }

    @Override
    public long onDiskSize()
    {
        return lastWrittenPos;
    }

    public static CompletableFuture<CommitLogSegment> create(CommitLog cl, Executor executor)
    {
        CompletableFuture<CommitLogSegment> retval = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> bufferFuture = createBufferListener(cl);
        Runnable handler = () -> {
            try
            {
                retval.complete(new CompressedSegment(cl, bufferFuture.get()));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        };
        //Try and process the future in this thread immediately if possible so waiters can retrieve the value
        //immediately from the future when it is first made visible.
        bufferFuture.thenRunAsync(handler, bufferFuture.isDone() ? MoreExecutors.directExecutor() : executor);
        return retval;
    }
}
