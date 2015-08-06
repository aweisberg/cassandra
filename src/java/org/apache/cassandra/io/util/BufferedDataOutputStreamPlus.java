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
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import net.nicoulaj.compilecommand.annotations.DontInline;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * An implementation of the DataOutputStreamPlus interface using a ByteBuffer to stage writes
 * before flushing them to an underlying channel.
 *
 * This class is completely thread unsafe.
 */
public class BufferedDataOutputStreamPlus extends DataOutputStreamPlus
{
    private static final int DEFAULT_BUFFER_SIZE = Integer.getInteger(Config.PROPERTY_PREFIX + "nio_data_output_stream_plus_buffer_size", 1024 * 32);

    protected ByteBuffer buffer;

    //Allow derived classes to specify writing to the channel
    //directly shouldn't happen because they intercept via doFlush for things
    //like compression or checksumming
    //Another hack for this value is that it also indicates that flushing early
    //should not occur, flushes aligned with buffer size are desired
    //Unless... it's the last flush. Compression and checksum formats
    //expect block (same as buffer size) alignment for everything except the last block
    protected boolean allowDirectWritesToChannel = true;

    //Cache the difference between native byte order and the order of the buffer
    //The slow path writes byte at a time and needs to do it correctly
    private final boolean swapBytes;

    public BufferedDataOutputStreamPlus(RandomAccessFile ras)
    {
        this(ras.getChannel());
    }

    public BufferedDataOutputStreamPlus(RandomAccessFile ras, int bufferSize)
    {
        this(ras.getChannel(), bufferSize);
    }

    public BufferedDataOutputStreamPlus(FileOutputStream fos)
    {
        this(fos.getChannel());
    }

    public BufferedDataOutputStreamPlus(FileOutputStream fos, int bufferSize)
    {
        this(fos.getChannel(), bufferSize);
    }

    public BufferedDataOutputStreamPlus(WritableByteChannel wbc)
    {
        this(wbc, DEFAULT_BUFFER_SIZE);
    }

    public BufferedDataOutputStreamPlus(WritableByteChannel wbc, int bufferSize)
    {
        this(wbc, ByteBuffer.allocateDirect(bufferSize));
        Preconditions.checkNotNull(wbc);
        Preconditions.checkArgument(bufferSize >= 8, "Buffer size must be large enough to accommodate a long/double");
    }

    protected BufferedDataOutputStreamPlus(WritableByteChannel channel, ByteBuffer buffer)
    {
        super(channel);
        this.buffer = buffer;
        swapBytes = ByteOrder.nativeOrder() == buffer.order();
    }

    protected BufferedDataOutputStreamPlus(ByteBuffer buffer)
    {
        super();
        this.buffer = buffer;
        swapBytes = ByteOrder.nativeOrder() == buffer.order();
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
                doFlush();
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
        if (toWrite.hasArray())
        {
            write(toWrite.array(), toWrite.arrayOffset() + toWrite.position(), toWrite.remaining());
        }
        else
        {
            assert toWrite.isDirect();
            final int toWriteRemaining = toWrite.remaining();
            if (toWriteRemaining > buffer.remaining())
            {
                if (allowDirectWritesToChannel)
                    doFlush();

                MemoryUtil.duplicateDirectByteBuffer(toWrite, hollowBuffer);
                int bufferRemaining = buffer.remaining();
                if (toWrite.remaining() > bufferRemaining && allowDirectWritesToChannel)
                {
                    while (hollowBuffer.hasRemaining())
                        channel.write(hollowBuffer);
                }
                else if (bufferRemaining >= toWriteRemaining)
                {
                    buffer.put(hollowBuffer);
                }
                else
                {
                    //Slow path when we aren't allow to flush to the channel directly because the derived class intercepts
                    //writes and does something such as compress or checksum
                    while(hollowBuffer.hasRemaining())
                    {
                        int originalLimit = hollowBuffer.limit();
                        int toPut = Math.min(hollowBuffer.remaining(), buffer.remaining());
                        hollowBuffer.limit(hollowBuffer.position() + toPut);
                        buffer.put(hollowBuffer);
                        hollowBuffer.limit(originalLimit);
                        if (hollowBuffer.hasRemaining())
                            doFlush();
                    }
                }
            }
            else
            {
                MemoryUtil.duplicateDirectByteBuffer(toWrite, hollowBuffer);
                buffer.put(hollowBuffer);
            }
        }
    }


    @Override
    public void write(int b) throws IOException
    {
        if (buffer.remaining() < 1)
            doFlush();
        buffer.put((byte) (b & 0xFF));
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        if (buffer.remaining() < 1)
            doFlush();
        buffer.put(v ? (byte)1 : (byte)0);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        write(v);
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        if (buffer.remaining() < 2)
            writeChar(v);
        else
            buffer.putShort((short) v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        if (buffer.remaining() < 2)
            writeCharSlow(v);
        else
            buffer.putChar((char) v);
    }

    @DontInline
    private void writeCharSlow(int v) throws IOException
    {
        if (swapBytes)
            v = Short.reverseBytes((short)v);
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        if (buffer.remaining() < 4)
            writeIntSlow(v);
        else
            buffer.putInt(v);
    }

    @DontInline
    private void writeIntSlow(int v) throws IOException
    {
        if (swapBytes)
            v = Integer.reverseBytes(v);
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        if (buffer.remaining() < 8)
            writeLongSlow(v);
        else
            buffer.putLong(v);
    }

    @DontInline
    private void writeLongSlow(long v) throws IOException
    {
        if (swapBytes)
            v = Long.reverseBytes(v);
        write((int) (v >>> 56) & 0xFF);
        write((int) (v >>> 48) & 0xFF);
        write((int) (v >>> 40) & 0xFF);
        write((int) (v >>> 32) & 0xFF);
        write((int) (v >>> 24) & 0xFF);
        write((int) (v >>> 16) & 0xFF);
        write((int) (v >>> 8) & 0xFF);
        write((int) (v >>> 0) & 0xFF);
    }


    @Override
    public void writeVInt(long value) throws IOException
    {
        writeUnsignedVInt(VIntCoding.encodeZigZag64(value));
    }

    @Override
    public void writeUnsignedVInt(long value) throws IOException
    {
        int size = VIntCoding.computeUnsignedVIntSize(value);
        if (size == 1)
        {
            write((int) value);
            return;
        }

        write(VIntCoding.encodeVInt(value, size), 0, size);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        if (buffer.remaining() < 4)
            writeFloatSlow(v);
        else
            buffer.putFloat(v);
    }

    @DontInline
    private void writeFloatSlow(float val) throws IOException
    {
        writeInt(Float.floatToIntBits(val));
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        if (buffer.remaining() < 8)
            writeDoubleSlow(v);
        else
            buffer.putDouble(v);
    }

    @DontInline
    private void writeDoubleSlow(double v) throws IOException
    {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            writeByte(s.charAt(index));
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            writeChar(s.charAt(index));
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        UnbufferedDataOutputStreamPlus.writeUTF(s, this);
    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    @DontInline
    protected void doFlush() throws IOException
    {
        buffer.flip();

        while (buffer.hasRemaining())
            channel.write(buffer);

        buffer.clear();
    }

    @Override
    public void flush() throws IOException
    {
        doFlush();
    }

    @Override
    public void close() throws IOException
    {
        doFlush();
        channel.close();
        FileUtils.clean(buffer);
        buffer = null;
    }

    @Override
    public <R> R applyToChannel(Function<WritableByteChannel, R> f) throws IOException
    {
        if (!allowDirectWritesToChannel)
            throw new UnsupportedOperationException();
        //Don't allow writes to the underlying channel while data is buffered
        flush();
        return f.apply(channel);
    }

    public BufferedDataOutputStreamPlus order(ByteOrder order)
    {
        this.buffer.order(order);
        return this;
    }
}
