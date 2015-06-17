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
// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package org.apache.cassandra.utils.vint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Borrows idea from
 * https://developers.google.com/protocol-buffers/docs/encoding#varints
 */
public class VIntCoding
{

    static final boolean debug = false;

    static void print(String s)
    {
        if (debug)
            System.out.print(s);
    }

    static void println(String s)
    {
        if (debug)
            System.out.println(s);
    }

    static String toString (long l)
    {
        String s = padToEight(Long.toBinaryString(l & 0xff));
        for (int ii = 1; ii < 8; ii++)
        {
            s += " " + padToEight(Long.toBinaryString((l >> 8 * ii) & 0xff));
        }
        return s;
    }

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    public static long readUnsignedVInt(DataInput input) throws IOException {
        byte firstByte = input.readByte();
        println("First byte " + padToEight(Integer.toBinaryString(firstByte & 0xff)));

        //Bail out early if this is one byte, necessary or it fails later
        if ((firstByte & 1 << 7) == 0)
            return firstByte & ~(1 << 7);

        final int mask = 0xffffffff;
        println("For number of leading zeroes " + padToEight(Integer.toBinaryString(firstByte ^ mask)));
        int size = Integer.numberOfLeadingZeros(firstByte ^ mask) - 24;
        println("Number of leading 0s " + size);

        long shift = 0;
        long retval = 0;
        if (size < 8)
        {
            println("&ing retval with mask " + toString(~masks[size + 1] & 0xff));
            retval = firstByte & (~masks[size + 1] & 0xff);
            println("Retval starts as " + toString(retval));
            shift = 7 - size;
        }

        for (int ii = 0; ii < size; ii++)
        {
            byte b = input.readByte();
            println("Incorporating byte " + padToEight(Long.toBinaryString(b & 0xff)));
            println("Incorporated as " + toString(((long)b & 0xffL) << shift));
            println("Shift is " + shift);
            retval |= ((long)b & 0xffL) << shift;
            println("Retval is " + toString(retval));
            shift += 8;
        }

        return retval;
    }

    public static long readVInt(DataInput input) throws IOException {
        return decodeZigZag64(readUnsignedVInt(input));
    }

    static final long masks[] = new long[9];

    static
    {
        long val = 0;

        masks[0] = val;

        val |= 1 << 7;
        masks[1] = val;

        val |= 1 << 6;
        masks[2] = val;

        val |= 1 << 5;
        masks[3] = val;

        val |= 1 << 4;
        masks[4] = val;

        val |= 1 << 3;
        masks[5]= val;

        val |= 1 << 2;
        masks[6] = val;

        val |= 1 << 1;
        masks[7] = val;

        val |= 1;
        masks[8] = val;
        for (long mask : masks) {
            println(toString(mask));
        }
    }

    static String padToEight(String s)
    {
        while (s.length() < 8)
        {
            s = "0" + s;
        }
        return s;
    }

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    public static void writeUnsignedVInt(long value, DataOutput output) throws IOException {
        int size = computeUnsignedVIntSize(value);
        println("Value " + toString(value));

        long baseMask = masks[size - 1];
        println("Base mask " + toString(baseMask));
        long zeroMask = ~(1L << (8 - size));
        println("Zero mask " + toString(baseMask));
        int firstByte = (int)((value | baseMask) & zeroMask) & 0xff;
        output.writeByte(firstByte);
        print("Code  " + padToEight(Long.toBinaryString(firstByte & 0xff)));
        if (firstByte != 255)
            value = value >> (8 - size);

        for (int ii = 0; ii < size - 1; ii++)
        {
            int b = (int)(value >> (ii * 8));
            output.writeByte(b);
            print(" " + padToEight(Long.toBinaryString(b & 0xff)));
        }
        print("\n");
        System.out.flush();
    }

    public static void writeVInt(long value, DataOutput output) throws IOException {
        writeUnsignedVInt(encodeZigZag64(value), output);
    }

    /**
     * Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n An unsigned 64-bit integer, stored in a signed int because
     *          Java has no explicit unsigned support.
     * @return A signed 64-bit integer.
     */
    public static long decodeZigZag64(final long n) {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    public static long encodeZigZag64(final long n) {
        // Note:  the right-shift must be arithmetic
        return (n << 1) ^ (n >> 63);
    }

    /** Compute the number of bytes that would be needed to encode a varint. */
    public static int computeVIntSize(final long param) {
        return computeUnsignedVIntSize(encodeZigZag64(param));
    }

    /** Compute the number of bytes that would be needed to encode an unsigned varint. */
    public static int computeUnsignedVIntSize(final long value) {
        if ((value & (1 << 7)) != 0)
            return Math.max(2, 9 - (Long.numberOfLeadingZeros(value) / 7));
        else
            return Math.max(1, 9 - (Long.numberOfLeadingZeros(value) / 7));
    }
}
