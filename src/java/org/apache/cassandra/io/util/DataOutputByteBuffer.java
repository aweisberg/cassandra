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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;


/**
 * An implementation of the DataOutputStream interface using a FastByteArrayOutputStream and exposing
 * its buffer so copies can be avoided.
 *
 * This class is completely thread unsafe.
 */
public class DataOutputByteBuffer extends DataOutputStreamAndChannelPlus
{

    ByteBuffer buffer;

    public DataOutputByteBuffer(ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public void write(int b) throws IOException
    {
        ensureRemaining(1);
        buffer.put((byte) (b & 0xFF));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        ensureRemaining(len);
        buffer.put(b, off, len);
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        int len = buffer.remaining();
        ensureRemaining(len);
        ByteBufferUtil.arrayCopy(buffer, buffer.position(), this.buffer, this.buffer.position(), len);
        this.buffer.position(this.buffer.position() + len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        ensureRemaining(1);
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
        ensureRemaining(2);
        buffer.putShort((short)v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        ensureRemaining(2);
        buffer.putChar((char)v);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        ensureRemaining(4);
        buffer.putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        ensureRemaining(8);
        buffer.putLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        ensureRemaining(4);
        buffer.putFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        ensureRemaining(8);
        buffer.putDouble(v);
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
        AbstractDataOutputStreamAndChannelPlus.writeUTF(s, this);
    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    protected void ensureRemaining(int minimum) throws IOException {}

    @Override
    public void flush() throws IOException {}
}
