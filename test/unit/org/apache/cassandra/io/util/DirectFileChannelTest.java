/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

public class DirectFileChannelTest
{

    ByteBuffer contents;

    private File createTempFile(int length) throws Exception  {
        final File retval = File.createTempFile("foo", "bar");
        retval.deleteOnExit();

        Random r = new Random(42);
        try (FileOutputStream fos = new FileOutputStream(retval))
        {
            contents = ByteBuffer.allocate(length);

            contents.clear();
            r.nextBytes(contents.array());

            while (contents.hasRemaining())
            {

                fos.getChannel().write(contents);
            }
            contents.flip();
        }
        return retval;
    }

    @Test
    public void testReadFile() throws Exception
    {
        final int length = 1024 * 1024 * 16 - 42;
        FileInputStream fis = new FileInputStream(createTempFile(length));
        DirectFileChannel dfc = new DirectFileChannel(fis.getChannel(), fis.getFD());

        ByteBuffer buf = ByteBuffer.allocate(42);

        while (contents.hasRemaining())
        {
           buf.clear();
           int read = dfc.read(buf);
           assertTrue(read <= contents.remaining());
           assertTrue(read > 0);

           buf.flip();
           while (buf.hasRemaining()) {
               final byte found = buf.get();
               final byte expected = contents.get();
               assertEquals(found, expected);
           }
        }
        buf.clear();
        assertTrue(dfc.read(buf) == -1);

        fis.close();
        dfc.close();
    }
}
