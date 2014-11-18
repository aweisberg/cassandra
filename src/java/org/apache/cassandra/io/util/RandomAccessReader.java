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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CLibrary;

public class RandomAccessReader extends AbstractDataInput implements FileDataInput
{
    public static final boolean USE_DIRECT_IO =
            Boolean.valueOf(System.getProperty("cassandra.use_direct_io", CLibrary.PLATFORM.directIOSupported ? "true" : "false"));

    /*
     * Direct file channel allocates a lot of memory and also doesn't emulate page cache like
     * behavior so it's not the best idea to have an unlimited number of them. Bound the
     * maximum number of active instances to this tunable.
     */
    public static final int MAX_DFCS = Integer.getInteger("cassandra.max_direct_file_channels", 64);

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // absolute filesystem path to the file
    private final String filePath;

    // buffer which will cache file blocks
    protected ByteBuffer buffer;

    // `bufferOffset` is the offset of the beginning of the buffer
    // `markedPointer` folds the offset of the last file mark
    protected long bufferOffset, markedPointer;

    // channel linked with the file, used to retrieve data and force updates.
    protected final FileChannel channel;

    private final long fileLength;

    protected final PoolingSegmentedFile owner;



    protected RandomAccessReader(File file, int bufferSize, PoolingSegmentedFile owner, RateLimiter limiter, boolean tryDirectIO) throws FileNotFoundException
    {
        this(file, bufferSize, false, owner, limiter, tryDirectIO);
    }

    protected RandomAccessReader(File file, int bufferSize, boolean useDirectBuffer, PoolingSegmentedFile owner, RateLimiter limiter, boolean tryDirectIO) throws FileNotFoundException
    {
        this.owner = owner;

        filePath = file.getAbsolutePath();

        try
        {
            //Should be safe to ignore since the FileChannel can be used to close it.
            //FileChannelImpl also retains a reference to the FIS so finalization is also not an issue
            @SuppressWarnings("resource")
            final FileInputStream fis = new FileInputStream(file);
            final FileChannel delegate = fis.getChannel();
            if (tryDirectIO && USE_DIRECT_IO && DirectFileChannel.NUM_DFCS.get() <= MAX_DFCS) {
                channel = new DirectFileChannel(delegate, fis.getFD(), limiter);
            } else if (limiter != null) {
                channel = new ThrottledFileChannel(delegate, limiter);
            } else {
                channel = delegate;
            }
        }
        catch (IOException e)
        {
            throw new FileNotFoundException(filePath);
        }

        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");

        // we can cache file length in read-only mode
        try
        {
            fileLength = channel.size();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
        buffer = allocateBuffer(bufferSize, useDirectBuffer);
        buffer.limit(0);
    }

    protected ByteBuffer allocateBuffer(int bufferSize, boolean useDirectBuffer)
    {
        int size = (int) Math.min(fileLength, bufferSize);
        return useDirectBuffer
                ? ByteBuffer.allocate(size)
                : ByteBuffer.allocateDirect(size);
    }

    public static RandomAccessReader open(File file, PoolingSegmentedFile owner)
    {
        return open(file, DEFAULT_BUFFER_SIZE, owner, null, false);
    }

    public static RandomAccessReader open(File file)
    {
        return open(file, DEFAULT_BUFFER_SIZE, null, null, false);
    }

    public static RandomAccessReader openDirect(File file)
    {
        return open(file, DEFAULT_BUFFER_SIZE, null, null, true);
    }

    public static RandomAccessReader openDirect(File file, RateLimiter limiter)
    {
        return open(file, DEFAULT_BUFFER_SIZE, null, limiter, true);
    }

    @VisibleForTesting
    static RandomAccessReader open(File file, int bufferSize, PoolingSegmentedFile owner, RateLimiter limiter, boolean tryDirectIO)
    {
        try
        {
            return new RandomAccessReader(file, bufferSize, owner, limiter, tryDirectIO);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static RandomAccessReader open(SequentialWriter writer)
    {
        return open(new File(writer.getPath()), DEFAULT_BUFFER_SIZE, null, null, false);
    }

    // channel extends FileChannel, impl SeekableByteChannel.  Safe to cast.
    public FileChannel getChannel()
    {
        return channel;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer()
    {
        bufferOffset += buffer.position();
        buffer.clear();
        assert bufferOffset < fileLength;

        try
        {
            channel.position(bufferOffset); // setting channel position
            while (buffer.hasRemaining())
            {
                int n = channel.read(buffer);
                if (n < 0)
                    break;
            }
            buffer.flip();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    @Override
    public long getFilePointer()
    {
        return current();
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public String getPath()
    {
        return filePath;
    }

    public int getTotalBufferSize()
    {
        //This may NPE so we make a ref
        //https://issues.apache.org/jira/browse/CASSANDRA-7756
        ByteBuffer ref = buffer;
        return ref != null ? ref.capacity() : 0;
    }

    public void reset()
    {
        seek(markedPointer);
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public FileMark mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return getFilePointer() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    @Override
    public void close()
    {
        if (owner == null || buffer == null)
        {
            // The buffer == null check is so that if the pool owner has deallocated us, calling close()
            // will re-call deallocate rather than recycling a deallocated object.
            // I'd be more comfortable if deallocate didn't have to handle being idempotent like that,
            // but RandomAccessFile.close will call AbstractInterruptibleChannel.close which will
            // re-call RAF.close -- in this case, [C]RAR.close since we are overriding that.
            deallocate();
        }
        else
        {
            owner.recycle(this);
        }
    }

    public void deallocate()
    {
        bufferOffset += buffer.position();
        FileUtils.clean(buffer);

        buffer = null; // makes sure we don't use this after it's ostensibly closed

        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + filePath + "')";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark
    {
        final long pointer;

        public BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (newPosition >= length()) // it is save to call length() in read-only mode
        {
            if (newPosition > length())
                throw new IllegalArgumentException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, getPath(), length()));
            buffer.limit(0);
            bufferOffset = newPosition;
            return;
        }

        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }
        // Set current location to newPosition and clear buffer so reBuffer calculates from newPosition
        bufferOffset = newPosition;
        buffer.clear();
        reBuffer();
        assert current() == newPosition;
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read()
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (!buffer.hasRemaining())
            reBuffer();

        return (int)buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] buffer)
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length)
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (!buffer.hasRemaining())
            reBuffer();

        int toCopy = Math.min(length, buffer.remaining());
        buffer.get(buff, offset, toCopy);
        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws EOFException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;
        try
        {
            ByteBuffer result = ByteBuffer.allocate(length);
            while (result.hasRemaining())
            {
                if (isEOF())
                    throw new EOFException();
                if (!buffer.hasRemaining())
                    reBuffer();
                ByteBufferUtil.put(buffer, result);
            }
            result.flip();
            return result;
        }
        catch (EOFException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public long length()
    {
        return fileLength;
    }

    public long getPosition()
    {
        return bufferOffset + buffer.position();
    }

    public long getPositionLimit()
    {
        return length();
    }
}
