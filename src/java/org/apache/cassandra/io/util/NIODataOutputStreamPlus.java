package org.apache.cassandra.io.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Preconditions;

/**
 * Rough equivalent of BufferedOutputStream and DataOutputStreamPlus wrapping the output stream of a file or socket.
 * Created to work around the fact that when BOS + DOS delegate to NIO for a socket they allocate large
 * thread local direct byte buffers when a large array used to write.
 *
 * There may also be some performance improvement due to using a DBB as the underlying buffer for IO and the removal
 * of some indirection and delegation when it comes to reading out individual values, but that is not the goal.
 *
 * Closing NIODataOutputStreamPlus will invoke close on the WritableByteChannel provided at construction.
 */
public class NIODataOutputStreamPlus extends OutputStream implements DataOutputPlus
{
    private final ByteBuffer buf;
    private final WritableByteChannel wbc;

    public NIODataOutputStreamPlus(WritableByteChannel wbc, int bufferSize)
    {
        Preconditions.checkNotNull(wbc);
        Preconditions.checkArgument(bufferSize >= 8, "Buffer size must be large enough to accomadate a long/double");
        this.wbc = wbc;
        buf = ByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    public void flush() throws IOException
    {
        buf.flip();

        while (buf.hasRemaining())
            wbc.write(buf);

        buf.clear();
    }

    @Override
    public void close() throws IOException
    {
        wbc.close();
    }

    private void ensureRemaining(int minimum) throws IOException
    {
        if (buf.remaining() < minimum)
            flush();
    }

    @Override
    public void write(int b) throws IOException
    {
        if (!buf.hasRemaining())
            flush();
        buf.put((byte)(b & 0xFF));
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
            if (buf.hasRemaining())
            {
                int toCopy = Math.min(len - copied, buf.remaining());
                buf.put(b, off + copied, toCopy);
                copied += toCopy;
            }
            else
            {
                flush();
            }
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        if (!buf.hasRemaining())
            flush();
        buf.put(v ? (byte)1 : (byte)0);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        write(v);
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        ensureRemaining(2);
        buf.putShort((short)v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        ensureRemaining(2);
        buf.putChar((char)v);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        ensureRemaining(4);
        buf.putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        ensureRemaining(8);
        buf.putLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        ensureRemaining(4);
        buf.putFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        ensureRemaining(8);
        buf.putDouble(v);
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
        int utfCount = 0, length = s.length();
        for (int i = 0; i < length; i++)
        {
            int charValue = s.charAt(i);
            if (charValue > 0 && charValue <= 127)
                utfCount++;
            else if (charValue <= 2047)
                utfCount += 2;
            else
                utfCount += 3;
        }

        if (utfCount > 65535)
            throw new UTFDataFormatException(); //$NON-NLS-1$

        /*
         * Is it possible to do a no allocation/copy fast path here
         * for strings that already fit in the buffer? Maybe also skip the
         * counting characters pass as well since ByteBuffer is already bounds checking
         */
        byte utfBytes[] = new byte[utfCount + 2];
        int utfIndex = 2;
        for (int i = 0; i < length; i++)
        {
            int charValue = s.charAt(i);
            if (charValue > 0 && charValue <= 127)
            {
                utfBytes[utfIndex++] = (byte) charValue;
            }
            else if (charValue <= 2047)
            {
                utfBytes[utfIndex++] = (byte) (0xc0 | (0x1f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            }
            else
            {
                utfBytes[utfIndex++] = (byte) (0xe0 | (0x0f & (charValue >> 12)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            }
        }
        utfBytes[0] = (byte) (utfCount >> 8);
        utfBytes[1] = (byte) utfCount;
        write(utfBytes);
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        if (buffer.isDirect() && buffer.remaining() > buf.remaining())
        {
            flush();
            while (buffer.hasRemaining())
                wbc.write(buffer);
        }
        else
        {
            while (buffer.hasRemaining())
                writeNext(buffer);
        }
    }

    private void writeNext(ByteBuffer buffer) throws IOException
    {
        if (!buf.hasRemaining())
            flush();
        assert(buf.hasRemaining());

        int originalLimit = buffer.limit();
        int toCopy = Math.min(buf.remaining(), buffer.remaining());

        buffer.limit(buffer.position() + toCopy);
        buf.put(buffer);

        buffer.limit(originalLimit);

    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

}
