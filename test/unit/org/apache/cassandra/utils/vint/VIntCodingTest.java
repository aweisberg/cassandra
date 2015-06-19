/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.vint;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.junit.Test;

import junit.framework.Assert;

public class VIntCodingTest
{


    static int alternateSize(long value)
    {
        long leadingZeroes = Long.numberOfLeadingZeros(value);
        if (leadingZeroes > 0 + 56 + 1) return 1;
        if (leadingZeroes > 1 + 48 + 1) return 2;
        if (leadingZeroes > 2 + 40 + 1) return 3;
        if (leadingZeroes > 3 + 32 + 1) return 4;
        if (leadingZeroes > 4 + 24 + 1) return 5;
        if (leadingZeroes > 5 + 16 + 1) return 6;
        if (leadingZeroes > 6 + 8 + 1) return 7;
        if (leadingZeroes > 7 + 1) return 8;
        return 9;
    }

    @Test
    public void testComputeSize() throws Exception
    {
        assertEquals(alternateSize(0L), VIntCoding.computeUnsignedVIntSize(0L));

        long val = 0;
        for (int ii = 0; ii < 64; ii++) {
            val |= 1L << ii - 1;
            int expectedSize = alternateSize(val);
            assertEquals( expectedSize, VIntCoding.computeUnsignedVIntSize(val));
            assertEncodedAtExpectedSize(val, expectedSize);
        }
    }

    private void assertEncodedAtExpectedSize(long value, int expectedSize) throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        VIntCoding.writeUnsignedVInt(value, dos);
        dos.flush();
        assertEquals( expectedSize, baos.toByteArray().length);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt(value);
        assertEquals( expectedSize, dob.buffer().remaining());
        dob.close();
    }

    @Test
    public void testReadExtraBytesCount()
    {
        for (int i = 1 ; i < 8 ; i++)
            Assert.assertEquals(i, VIntCoding.numberOfExtraBytesToRead((byte) ((0xFF << (8 - i)) & 0xFF)));
    }

}
