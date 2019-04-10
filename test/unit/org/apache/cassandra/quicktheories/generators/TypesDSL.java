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

package org.apache.cassandra.quicktheories.generators;

import java.nio.ByteBuffer;

import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;

import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.characters;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;

public class TypesDSL
{
    public static Gen<String> asciiGen = new Gen<String>()
    {
        private final Gen<Integer> lenghGenerator = integers().between(1, 5);
        private final Gen<Character> charGen = characters().basicLatinCharacters();

        public String generate(RandomnessSource in)
        {
            int size = lenghGenerator.generate(in);

            char[] chars = new char[size];
            for (int i = 0; i < size; i++)
            {
                chars[i] = charGen.generate(in);
            }

            return new String(chars, 0, size);
        }
    };

    public static Gen<Long> longGen = longs().all();

    public static Gen<ByteBuffer> blobGen = new Gen<ByteBuffer>()
    {

        // TODO (alexp): remove arbitrary limits
        private final Gen<Integer> lenghGenerator = integers().between(0, 10000);
        private final Gen<Character> charGen = characters().ascii();

        public ByteBuffer generate(RandomnessSource in)
        {
            int size = lenghGenerator.generate(in);

            byte[] bytes = new byte[size];
            for (int i = 0; i < size; i++)
            {
                bytes[i] = (byte) charGen.generate(in).charValue();
            }

            return ByteBuffer.wrap(bytes);
        }
    };

    public static Gen<Boolean> booleanGen = booleans().all();

    public static Gen<Integer> dateGen = integers().all();

    public static Gen<String> asciiGen()
    {
        return asciiGen;
    }

    // TODO: other types
}
