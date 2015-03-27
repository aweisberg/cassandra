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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.memory.MemoryUtil;

import com.google.common.base.Preconditions;

/**
 * Rough equivalent of BufferedOutputStream and DataOutputStreamPlus wrapping the output stream of a file or socket.
 * Created to work around the fact that when BOS + DOS delegate to NIO for a socket they allocate large
 * thread local direct byte buffers when a large array used to write.
 *
 * There may also be some performance improvement due to using a DBB as the underlying buffer for IO and the removal
 * of some indirection and delegation when it comes to reading out individual values, but that is not the goal.
 *
 * Closing NIODataOutputStreamAndChannelPlus will invoke close on the WritableByteChannel provided at construction.
 *
 * NIODataOutputStreamAndChannelPlus is not thread safe.
 */
public class NIODataOutputStreamAndChannelPlus extends DataOutputStreamByteBufferAndChannelPlus
{
    private static final int DEFAULT_BUFFER_SIZE = Integer.getInteger(Config.PROPERTY_PREFIX + "nio_data_output_stream_plus_buffer_size", 1024 * 32);
    private final WritableByteChannel wbc;

    public NIODataOutputStreamAndChannelPlus(RandomAccessFile ras)
    {
        this(ras.getChannel());
    }

    public NIODataOutputStreamAndChannelPlus(RandomAccessFile ras, int bufferSize)
    {
        this(ras.getChannel(), bufferSize);
    }

    public NIODataOutputStreamAndChannelPlus(FileOutputStream fos)
    {
        this(fos.getChannel());
    }

    public NIODataOutputStreamAndChannelPlus(FileOutputStream fos, int bufferSize)
    {
        this(fos.getChannel(), bufferSize);
    }

    public NIODataOutputStreamAndChannelPlus(WritableByteChannel wbc)
    {
        this( wbc, DEFAULT_BUFFER_SIZE);
    }

    public NIODataOutputStreamAndChannelPlus(WritableByteChannel wbc, int bufferSize)
    {
        super(ByteBuffer.allocateDirect(bufferSize));
        Preconditions.checkNotNull(wbc);
        Preconditions.checkArgument(bufferSize >= 8, "Buffer size must be large enough to accomadate a long/double");
        this.wbc = wbc;
    }

    @Override
    public void flush() throws IOException
    {
        buffer.flip();

        while (buffer.hasRemaining())
            wbc.write(buffer);

        buffer.clear();
    }

    @Override
    public void close() throws IOException
    {
        flush();
        wbc.close();
        FileUtils.clean(buffer);
        buffer = null;
    }

    @Override
    protected void ensureRemaining(int minimum) throws IOException
    {
        if (buffer.remaining() < minimum)
            flush();
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        if (b == null)
            throw new NullPointerException();

        // avoid int overflow
        if (off < 0 || off > b.length || len < 0
                || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return;

        int copied = 0;
        while (copied < len)
        {
            if (buffer.hasRemaining())
            {
                int toCopy = Math.min(len - copied, buffer.remaining());
                buffer.put(b, off + copied, toCopy);
                copied += toCopy;
            }
            else
            {
                flush();
            }
        }
    }

    // ByteBuffer to use for defensive copies
    private final ByteBuffer hollowBuffer = MemoryUtil.getHollowDirectByteBuffer();

    /*
     * Makes a defensive copy of the incoming ByteBuffer and don't modify the position or limit
     * even temporarily so it is thread-safe WRT to the incoming buffer
     * (non-Javadoc)
     * @see org.apache.cassandra.io.util.DataOutputPlus#write(java.nio.ByteBuffer)
     */
    @Override
    public void write(ByteBuffer toWrite) throws IOException
    {
        if (toWrite.isDirect() && toWrite.remaining() > buffer.remaining())
        {
            flush();
            MemoryUtil.duplicateByteBuffer(toWrite, hollowBuffer);
            while (hollowBuffer.hasRemaining())
                wbc.write(hollowBuffer);
        }
        else if (toWrite.isDirect())
        {
            MemoryUtil.duplicateByteBuffer(toWrite, hollowBuffer);
            buffer.put(hollowBuffer);
        }
        else
        {
            write(toWrite.array(), toWrite.arrayOffset() + toWrite.position(), toWrite.remaining());
        }
    }

    @Override
    protected WritableByteChannel channel()
    {
        return wbc;
    }
}
