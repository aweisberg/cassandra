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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class DelegatingFileChannel extends FileChannel
{

    protected final FileChannel fc;

    public DelegatingFileChannel(FileChannel fc) {
        this.fc = fc;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        return fc.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
    {
        return fc.read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        return fc.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
        return fc.write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException
    {
        return fc.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException
    {
        fc.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException
    {
        return fc.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException
    {
        fc.truncate(size);
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
    public int read(ByteBuffer dst, long position) throws IOException
    {
        return fc.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException
    {
        return fc.write(src, position);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException
    {
        return fc.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException
    {
        return fc.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException
    {
        return fc.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException
    {
        fc.close();
    }
}
