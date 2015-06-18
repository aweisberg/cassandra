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

    public static boolean debug = false;

    public static void print(String s)
    {
        if (debug)
            System.out.print(s);
    }

    public static void println(String s)
    {
        if (debug)
            System.out.println(s);
    }

    public static String toString (long l)
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
            println("&ing retval with mask " + toString(~lengthExtensionMasks[size + 1] & 0xff));
            retval = firstByte & (~lengthExtensionMasks[size + 1] & 0xff);
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


    public static long truncationMask(int pivot)
    {
        return ((1L << (pivot + 1)) - 1);
    }

    public static final long lengthExtensionMasks[] = new long[9];

    static
    {
        long val = 0;

        lengthExtensionMasks[0] = val;

        for (int ii = 7; ii >= 0; ii--)
        {
            val |= 1 << ii;
            lengthExtensionMasks[8 - ii] = val;
        }

        for (long mask : lengthExtensionMasks) {
            println(toString(mask));
        }
    }

    public static String padToEight(String s)
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
        println("Size " + size);

        long baseMask = lengthExtensionMasks[size - 1];
        println("Base mask " + toString(baseMask));
        long zeroMask = ~(1L << (8 - size));
        println("Zero mask " + toString(baseMask));
        int firstByte = (int)((value | baseMask) & zeroMask) & 0xff;
        output.writeByte(firstByte);
        print("Code  " + padToEight(Long.toBinaryString(firstByte & 0xff)));

        //Lost one bit per byte and a padding 0 bit
        if (firstByte != (-1 & 0xff))
            value = value >> (8 - size);

        for (int ii = 0; ii < size - 1; ii++)
        {
            int b = (int)(value >> (ii * 8));
            //A varint that encodes zeroes is not very useful
            assert((ii + 1 < size) || ((b & 0xff) != 0));
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
        //return (8 - (Long.numberOfLeadingZeros(value) / 8)) + 1;
    }
}
