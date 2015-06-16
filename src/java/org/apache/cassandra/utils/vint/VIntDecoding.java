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
package org.apache.cassandra.utils.vint;

import java.io.DataInput;
import java.io.IOException;

/**
 * Borrows idea from
 * https://developers.google.com/protocol-buffers/docs/encoding#varints
 */
public class VIntDecoding
{
    public static long vintDecode(DataInput input) throws IOException
    {
        byte firstByte = input.readByte();
        if (firstByte >= -112)
            return firstByte;

        int len = vintDecodeSize(firstByte) - 1;

        long i = 0;
        for (int idx = 0; idx < len; idx++)
        {
            byte b = input.readByte();
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (vintIsNegative(firstByte) ? (i ^ -1L) : i);
    }

    /*
     * Does not decode size 1
     */
    public static int vintDecodeSize(byte value)
    {
        return value < -120 ? -119 - value : -111 - value;
    }

    /*
     * This assumes that you have already discarded values > -112 which are indeed negative
     */
    public static boolean vintIsNegative(byte value)
    {
        assert(value < -112);
        return value < -120;
    }
}
