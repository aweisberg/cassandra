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

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    public static long readUnsignedVInt(DataInput input) throws IOException {
      long result = 0;
      for (int shift = 0; shift < 64; shift += 7) {
        final byte b = input.readByte();
        result |= (long) (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          return result;
        }
      }
      throw new RuntimeException("Malformed varint");
    }

    public static long readVInt(DataInput input) throws IOException {
        return decodeZigZag64(readUnsignedVInt(input));
    }

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    public static void writeUnsignedVInt(long value, DataOutput output) throws IOException {
      while (true) {
        if ((value & ~0x7FL) == 0) {
          output.write((int)value);
          return;
        } else {
          output.write(((int)value & 0x7F) | 0x80);
          value >>>= 7;
        }
      }
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
      final long value = encodeZigZag64(param);
      if ((value & (0xffffffffffffffffL <<  7)) == 0) return 1;
      if ((value & (0xffffffffffffffffL << 14)) == 0) return 2;
      if ((value & (0xffffffffffffffffL << 21)) == 0) return 3;
      if ((value & (0xffffffffffffffffL << 28)) == 0) return 4;
      if ((value & (0xffffffffffffffffL << 35)) == 0) return 5;
      if ((value & (0xffffffffffffffffL << 42)) == 0) return 6;
      if ((value & (0xffffffffffffffffL << 49)) == 0) return 7;
      if ((value & (0xffffffffffffffffL << 56)) == 0) return 8;
      if ((value & (0xffffffffffffffffL << 63)) == 0) return 9;
      return 10;
    }

    /** Compute the number of bytes that would be needed to encode an unsigned varint. */
    public static int computeUnsignedVIntSize(final long value) {
      if ((value & (0xffffffffffffffffL <<  7)) == 0) return 1;
      if ((value & (0xffffffffffffffffL << 14)) == 0) return 2;
      if ((value & (0xffffffffffffffffL << 21)) == 0) return 3;
      if ((value & (0xffffffffffffffffL << 28)) == 0) return 4;
      if ((value & (0xffffffffffffffffL << 35)) == 0) return 5;
      if ((value & (0xffffffffffffffffL << 42)) == 0) return 6;
      if ((value & (0xffffffffffffffffL << 49)) == 0) return 7;
      if ((value & (0xffffffffffffffffL << 56)) == 0) return 8;
      if ((value & (0xffffffffffffffffL << 63)) == 0) return 9;
      return 10;
    }
}
