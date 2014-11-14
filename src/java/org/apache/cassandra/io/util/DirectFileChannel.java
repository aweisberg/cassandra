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

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.utils.CLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/*
 * DirectFileChannel wraps a file channel object that is used as a delegate for IO requests. It uses the provided
 * FileDescriptor to enable O_DIRECT for the FileChannel via JNA and the fcntl system call. The DFC doesn't take
 * ownership of the underlying FileChannel and closing the DFC will not close the channel and the FD will continue
 * to have O_DIRECT enabled.
 *
 * Concurrency wise the DFS is not as concurrent as a FileChannel and uses the intrinsic lock for all read operations
 * to protect access to the internal buffers used to emit IOs and fill incoming read buffers. This holds even for
 * absolute reads (unlike FileChannel). The intrinsic lock also protects a few other operations like truncate
 * and position that might might interfere with each other or read operations.
 *
 * Write operations that can't be delegated to the FileChannel throw UnsupportedOperationException. Operations that
 * are delegated to the FileChannel like transferTo, transferFrom, map, lock, tryLock don't lock the intrinsic lock.
 *
 * Generally speaking don't use this class to do anything other than read from a file sequentially from a single
 * thread. If you have multiple threads then use multiple DFC instances with the same FD. Non-sequential access works
 * but the IOs may be too large if you ware doing small reads.
 *
 * The file cursor intrinsic to the FD is used for an initial position, but after that the DFC maintains its own
 * internal cursor to track position in the internal buffer and for subsequent reads. Changes to position
 * will be forwarded to the delegate in addition to changing the internal position.
 *
 * A finalizer is implemented as a last ditch attempt to reclaim the memory allocated for the internal buffer.
 */
public class DirectFileChannel extends FileChannel
{

    private static final Logger logger = LoggerFactory.getLogger(DirectFileChannel.class);

    static final long POSITION_INVALID = -1;


    /*
     * File channel to delegate IO operations to
     */
    final FileChannel fc;

    /*
     * Unligned memory allocation backing the page aligned buffer
     */
    final Memory origin;

    /*
     * Internal cursor used for file position instead of the one implicit in the FD
     */
    volatile long filePosition = 0;

    /*
     * Page aligned buffer to use for IO
     */
    final ByteBuffer buffer;

    /*
     * Logical position in file where the buffered data starts. Set to invalid when there is no data.
     */
    long bufferStartPosition = POSITION_INVALID;

    /*
     * Logical position in file where the buffered data ends
     */
    long bufferEndPosition = POSITION_INVALID;


    /*
     * FileChannel and FD should be the same file descriptor. Nothing breaks, but you won't
     * get the FD changed to use O_DIRECT if they aren't matched.
     *
     * If enabling O_DIRECT fails a warning is logged and the DFC continues to work albeit
     * inefficiently since it still does its own buffering.
     */
    public DirectFileChannel(FileChannel fc, FileDescriptor fd) throws IOException {
        this.fc = fc;

        if (!CLibrary.tryEnableODIRECT(fd))
        {
            logger.warn("Unable to enable O_DIRECT in DirectFileChannel");
        }

        origin = Memory.allocateAlignable(1024 * 1024 * 2);
        buffer = origin.asAlignedByteBuffer();
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException
    {
        int read = read(dst, filePosition);

        if (read != -1)
            filePosition += read;

        return read;
    }

    @Override
    public synchronized long read(ByteBuffer[] dsts, int offset, int length) throws IOException
    {
        Preconditions.checkNotNull(dsts);
        Preconditions.checkArgument(offset < dsts.length);
        Preconditions.checkArgument(offset + length < dsts.length);

        long totalRead = 0;
        int readLastTime = 0;
        for (int ii = offset; ii < offset + length; ii++)
        {
            final ByteBuffer buf = dsts[ii];
            readLastTime = read(buf);
            totalRead += Math.max(0, readLastTime);

            if (readLastTime == -1 || buf.hasRemaining())
                break;
        }
        if (totalRead == 0 && readLastTime == -1) return -1;
        return totalRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException
    {
        return filePosition;
    }

    @Override
    public synchronized FileChannel position(long newPosition) throws IOException
    {
        filePosition = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException
    {
        return fc.size();
    }

    @Override
    public synchronized FileChannel truncate(long size) throws IOException
    {
        bufferStartPosition = POSITION_INVALID;
        bufferEndPosition = POSITION_INVALID;
        fc.truncate(size);
        filePosition = fc.position();
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException
    {
        fc.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException
    {
        return fc.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException
    {
        return fc.transferFrom(src, position, count);
    }

    @Override
    public synchronized int read(ByteBuffer dst, long position) throws IOException
    {
        Preconditions.checkArgument(position >= 0);
        Preconditions.checkNotNull(dst);

        if (dst.remaining() == 0) return 0;

        int read = 0;
        while (dst.hasRemaining())
        {

            /*
             * Is there no data in the buffer?
             * Is the position being read from outside the buffer?
             * Go do an IO if necessary
             */
            if (bufferStartPosition == POSITION_INVALID ||
                    position < bufferStartPosition ||
                    position >= bufferEndPosition)
            {
                bufferStartPosition = POSITION_INVALID;
                bufferEndPosition = POSITION_INVALID;

                //Prep for an aligned IO
                buffer.clear();
                final int pageSize = NativeAllocator.pageSize();
                final int headSlack = (int)position % pageSize;
                final long ioStart = position - headSlack;

                //Both begin and end of the IO should be aligned
                //The end should be implicitly aligned due to the buffer being aligned
                assert(ioStart % pageSize== 0);
                assert((ioStart + buffer.remaining()) % pageSize == 0);

                final int readThisTime = fc.read(buffer, ioStart);

                buffer.flip();

                //Note exactly how much was read and what the buffer covers
                if (readThisTime > 0) {
                    bufferStartPosition = ioStart;
                    bufferEndPosition = ioStart + readThisTime;
                }

                //Adjust for the slack that was added so a legitimate answer is given
                //for whether any data was available
                buffer.position(Math.min(buffer.remaining(), headSlack));

                if (!buffer.hasRemaining() || readThisTime == -1)
                    break;
            }

            // Copy the data into the destination buffer
            final int oldlimit = buffer.limit();
            try
            {
                buffer.position((int)(position - bufferStartPosition));

                //Copy as many bytes as fit in the destination buffer or the number of bytes available
                //Whichever is less
                buffer.limit(Math.min(buffer.position() + dst.remaining(), buffer.limit()));

                read += buffer.remaining();
                position += read;
                dst.put(buffer);
            }
            finally
            {
                buffer.limit(oldlimit);
            }
        }

        return read == 0 ? -1 : read;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException
    {
        return fc.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException
    {
        return fc.lock();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException
    {
        return fc.tryLock(position, size, shared);
    }

    @Override
    protected synchronized void implCloseChannel() throws IOException
    {
        //implCloseChannel is protected by FileChannel from being called twice
        origin.free();
    }

    @Override
    public void finalize() {
        try
        {
            close();
        }
        catch (Exception e)
        {
            logger.warn("Exception closing DirectFileChannel from finalizer", e);
        }
    }
}


