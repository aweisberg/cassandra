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
import java.nio.channels.FileChannel;

import com.google.common.util.concurrent.RateLimiter;

public class ThrottledFileChannel extends DelegatingFileChannel
{

    final RateLimiter limiter;

    public ThrottledFileChannel(FileChannel fc, RateLimiter limiter)
    {
        super(fc);
        this.limiter = limiter;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        final int read = super.read(dst);

        if (read > 0)
        {
            limiter.acquire(read);
        }

        return read;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
    {
        final long read = super.read(dsts, offset, length);
        if (read < 1) return read;

        int iterations = (int)(read / (long)Integer.MAX_VALUE);
        long remainder = read % (long)Integer.MAX_VALUE;
        limiter.acquire((int)remainder);
        for (int ii = 0; ii < iterations; ii++)
        {
            limiter.acquire(Integer.MAX_VALUE);
        }

        return read;
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        final int written = super.write(src);
        if (written > 0)
            limiter.acquire(written);
        return written;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
        final long written = super.write(srcs, offset, length);
        if (written < 1) return written;

        int iterations = (int)(written / (long)Integer.MAX_VALUE);
        long remainder = written % (long)Integer.MAX_VALUE;
        limiter.acquire((int)remainder);
        for (int ii = 0; ii < iterations; ii++)
        {
            limiter.acquire(Integer.MAX_VALUE);
        }

        return written;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException
    {
        final int read = super.read(dst, position);

        if (read > 0)
        {
            limiter.acquire(read);
        }
        return read;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException
    {
        final int written = super.write(src, position);

        if (written > 0)
        {
            limiter.acquire(written);
        }

        return written;
    }
}
