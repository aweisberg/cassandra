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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.memory.MemoryUtil;

import com.google.common.base.Function;

/**
 * Base class for DataOutput implementations that does not have an optimized implementations of Plus methods
 * and does no buffering.
 *
 * Unlike BufferedDataOutputStreamPlus this is capable of operating as an unbuffered output stream.
 * Currently necessary because SequentialWriter implements its own buffering along with mark/reset/truncate.
 */
public abstract class UnbufferedDataOutputStreamPlus extends DataOutputStreamPlus
{
    protected UnbufferedDataOutputStreamPlus()
    {
        super();
    }

    protected UnbufferedDataOutputStreamPlus(WritableByteChannel channel)
    {
        super(channel);
    }

    /*
    !! DataOutput methods below are copied from the implementation in Apache Harmony RandomAccessFile.
    */

    /**
     * Writes the entire contents of the byte array <code>buffer</code> to
     * this RandomAccessFile starting at the current file pointer.
     *
     * @param buffer
     *            the buffer to be written.
     *
     * @throws IOException
     *             If an error occurs trying to write to this RandomAccessFile.
     */
    public void write(byte[] buffer) throws IOException {
        write(buffer, 0, buffer.length);
    }

    /**
     * Writes <code>count</code> bytes from the byte array <code>buffer</code>
     * starting at <code>offset</code> to this RandomAccessFile starting at
     * the current file pointer..
     *
     * @param buffer
     *            the bytes to be written
     * @param offset
     *            offset in buffer to get bytes
     * @param count
     *            number of bytes in buffer to write
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             RandomAccessFile.
     * @throws IndexOutOfBoundsException
     *             If offset or count are outside of bounds.
     */
    public abstract void write(byte[] buffer, int offset, int count) throws IOException;

    /**
     * Writes the specified byte <code>oneByte</code> to this RandomAccessFile
     * starting at the current file pointer. Only the low order byte of
     * <code>oneByte</code> is written.
     *
     * @param oneByte
     *            the byte to be written
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             RandomAccessFile.
     */
    public abstract void write(int oneByte) throws IOException;

    /**
     * Writes a boolean to this output stream.
     *
     * @param val
     *            the boolean value to write to the OutputStream
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeBoolean(boolean val) throws IOException {
        write(val ? 1 : 0);
    }

    /**
     * Writes a 8-bit byte to this output stream.
     *
     * @param val
     *            the byte value to write to the OutputStream
     *
     * @throws java.io.IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeByte(int val) throws IOException {
        write(val & 0xFF);
    }

    /**
     * Writes the low order 8-bit bytes from a String to this output stream.
     *
     * @param str
     *            the String containing the bytes to write to the OutputStream
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeBytes(String str) throws IOException {
        byte bytes[] = new byte[str.length()];
        for (int index = 0; index < str.length(); index++) {
            bytes[index] = (byte) (str.charAt(index) & 0xFF);
        }
        write(bytes);
    }

    /**
     * Writes the specified 16-bit character to the OutputStream. Only the lower
     * 2 bytes are written with the higher of the 2 bytes written first. This
     * represents the Unicode value of val.
     *
     * @param val
     *            the character to be written
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeChar(int val) throws IOException {
        write((val >>> 8) & 0xFF);
        write((val >>> 0) & 0xFF);
    }

    /**
     * Writes the specified 16-bit characters contained in str to the
     * OutputStream. Only the lower 2 bytes of each character are written with
     * the higher of the 2 bytes written first. This represents the Unicode
     * value of each character in str.
     *
     * @param str
     *            the String whose characters are to be written.
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeChars(String str) throws IOException {
        byte newBytes[] = new byte[str.length() * 2];
        for (int index = 0; index < str.length(); index++) {
            int newIndex = index == 0 ? index : index * 2;
            newBytes[newIndex] = (byte) ((str.charAt(index) >> 8) & 0xFF);
            newBytes[newIndex + 1] = (byte) (str.charAt(index) & 0xFF);
        }
        write(newBytes);
    }

    /**
     * Writes a 64-bit double to this output stream. The resulting output is the
     * 8 bytes resulting from calling Double.doubleToLongBits().
     *
     * @param val
     *            the double to be written.
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeDouble(double val) throws IOException {
        writeLong(Double.doubleToLongBits(val));
    }

    /**
     * Writes a 32-bit float to this output stream. The resulting output is the
     * 4 bytes resulting from calling Float.floatToIntBits().
     *
     * @param val
     *            the float to be written.
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeFloat(float val) throws IOException {
        writeInt(Float.floatToIntBits(val));
    }

    /**
     * Writes a 32-bit int to this output stream. The resulting output is the 4
     * bytes, highest order first, of val.
     *
     * @param val
     *            the int to be written.
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public void writeInt(int val) throws IOException {
        write((val >>> 24) & 0xFF);
        write((val >>> 16) & 0xFF);
        write((val >>>  8) & 0xFF);
        write((val >>> 0) & 0xFF);
    }

    /**
     * Writes a 64-bit long to this output stream. The resulting output is the 8
     * bytes, highest order first, of val.
     *
     * @param val
     *            the long to be written.
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public void writeLong(long val) throws IOException {
        write((int)(val >>> 56) & 0xFF);
        write((int)(val >>> 48) & 0xFF);
        write((int)(val >>> 40) & 0xFF);
        write((int)(val >>> 32) & 0xFF);
        write((int)(val >>> 24) & 0xFF);
        write((int)(val >>> 16) & 0xFF);
        write((int)(val >>>  8) & 0xFF);
        write((int) (val >>> 0) & 0xFF);
    }

    /**
     * Writes the specified 16-bit short to the OutputStream. Only the lower 2
     * bytes are written with the higher of the 2 bytes written first.
     *
     * @param val
     *            the short to be written
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public void writeShort(int val) throws IOException {
        writeChar(val);
    }

    private static final ThreadLocal<byte[]> utfBytesLocal = new ThreadLocal<byte[]>() {
        @Override
        public byte[] initialValue()
        {
            return new byte[16];
        }
    };

    /**
     * Writes the specified String out in UTF format to the provided DataOutput
     *
     * @param str
     *            the String to be written in UTF format.
     * @param out
     *            the DataOutput to write the UTF encoded string to
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public static void writeUTF(String str, DataOutput out) throws IOException {
        int utfCount = 0, length = str.length();
        utfCount = calculateUTFLength(str, utfCount, length);

        if (utfCount > 65535)
            throw new UTFDataFormatException(); //$NON-NLS-1$

        byte[] utfBytes = retrieveOutputBuffer(utfCount + 2);

        if (utfCount + 2 < utfBytes.length)
            fastPathEncode(str, out, utfCount, length, utfBytes);
        else
            slowPathEncode(str, out, utfCount, length, utfBytes);
    }

    /*
     * Factored out into separate method to create more flexibility around inlining
     */
    private static int calculateUTFLength(String str, int utfCount, int length)
    {
        for (int i = 0; i < length; i++)
        {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127)
                utfCount++;
            else if (charValue <= 2047)
                utfCount += 2;
            else
                utfCount += 3;
        }
        return utfCount;
    }

    public static final int MAX_BUFFER_SIZE =
            Integer.getInteger(Config.PROPERTY_PREFIX + "oaciu.abstract_data_output_max_buffer_size", 1024 * 8);

    /*
     * Factored out into separate method to create more flexibility around inlining
     */
    private static byte[] retrieveOutputBuffer(int utfCount)
    {
        byte utfBytes[] = utfBytesLocal.get();
        if (utfBytes.length < utfCount)
        {
            utfBytes = new byte[Math.min(MAX_BUFFER_SIZE, utfCount * 2)];
            utfBytesLocal.set(utfBytes);
        }
        return utfBytes;
    }

    /*
     * Not knowing what the distribution of multi-byte values is, can't fill the entire buffer when encoding.
     * Opting to not fill the buffer so the loop body would stay simple and maybe run faster.
     * TODO microbenchmark checking in the loop whether there are three bytes of buffer space remaining, and is
     * that faster than not checking but not filling the entire buffer
     */
    private static void slowPathEncode(String str, DataOutput out, int utfCount, int length, byte[] utfBytes)
            throws IOException
    {
        int totalWritten = 0;
        int utfIndex = 0;
        out.writeShort(utfCount);
        int charIndex = 0;
        while (totalWritten < utfCount)
        {
            int nextLength = Math.min(utfBytes.length / 3, length - charIndex );
            for (int i = 0; i < nextLength; i++)
            {
                int charValue = str.charAt(charIndex);
                charIndex++;
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
            out.write(utfBytes, 0, utfIndex);
            totalWritten += utfIndex;
            utfIndex = 0;
        }
    }

    /*
     * Know it will fit the buffer so we can use the entire buffer
     */
    private static void fastPathEncode(String str, DataOutput out, int utfCount, int length, byte[] utfBytes)
            throws IOException
    {
        int utfIndex = 2;
        for (int i = 0; i < length; i++)
        {
            int charValue = str.charAt(i);
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
        out.write(utfBytes, 0, utfIndex);
    }

    static void writeUTFLegacy(String str, DataOutput out) throws IOException {
        int utfCount = 0, length = str.length();
        for (int i = 0; i < length; i++) {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                utfCount++;
            } else if (charValue <= 2047) {
                utfCount += 2;
            } else {
                utfCount += 3;
            }
        }
        if (utfCount > 65535) {
            throw new UTFDataFormatException(); //$NON-NLS-1$
        }
        byte utfBytes[] = new byte[utfCount + 2];
        int utfIndex = 2;
        for (int i = 0; i < length; i++) {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                utfBytes[utfIndex++] = (byte) charValue;
            } else if (charValue <= 2047) {
                utfBytes[utfIndex++] = (byte) (0xc0 | (0x1f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            } else {
                utfBytes[utfIndex++] = (byte) (0xe0 | (0x0f & (charValue >> 12)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            }
        }
        utfBytes[0] = (byte) (utfCount >> 8);
        utfBytes[1] = (byte) utfCount;
        out.write(utfBytes);
    }

    /**
     * Writes the specified String out in UTF format.
     *
     * @param str
     *            the String to be written in UTF format.
     *
     * @throws IOException
     *             If an error occurs attempting to write to this
     *             DataOutputStream.
     */
    public final void writeUTF(String str) throws IOException {
        writeUTF(str, this);
    }


    // ByteBuffer to use for defensive copies
    private final ByteBuffer hollowBuffer = MemoryUtil.getHollowDirectByteBuffer();

    @Override
    public void write(ByteBuffer buf) throws IOException
    {
        MemoryUtil.duplicateByteBuffer(buf, hollowBuffer);
        while (hollowBuffer.hasRemaining())
            channel.write(hollowBuffer);
    }

    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    @Override
    public <R> R applyToChannel(Function<WritableByteChannel, R> f) throws IOException
    {
        return f.apply(channel);
    }
}
