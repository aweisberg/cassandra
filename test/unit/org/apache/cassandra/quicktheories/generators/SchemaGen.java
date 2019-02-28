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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.BooleansDSL;
import org.quicktheories.generators.CharactersDSL;
import org.quicktheories.generators.IntegersDSL;
import org.quicktheories.generators.ListsDSL;
import org.quicktheories.generators.LongsDSL;

import static org.apache.cassandra.quicktheories.generators.Extensions.monotonicGen;
import static org.apache.cassandra.schema.ColumnMetadata.ClusteringOrder;

public class SchemaGen
{
    @SuppressWarnings("unchecked")
    public static <T> Gen<T> fromValues(Collection<T> allValues)
    {
        return fromValues((T[]) allValues.toArray());
    }

    public static <T> Gen<T> fromValues(T[] allValues)
    {
        return new IntegersDSL().between(0, allValues.length - 1)
                                .map(i -> allValues[i]);
    }

    public static Gen<String> asciiGen = new Gen<String>()
    {
        private final Gen<Integer> lenghGenerator = new IntegersDSL().between(1, 5);
        private final Gen<Character> charGen = new CharactersDSL().basicLatinCharacters();

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

    public static Gen<Long> longGen = new LongsDSL().all();

    public static Gen<ByteBuffer> blobGen = new Gen<ByteBuffer>()
    {

        private final Gen<Integer> lenghGenerator = new IntegersDSL().between(0, 10000);
        private final Gen<Character> charGen = new CharactersDSL().ascii();

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

    public static Gen<Boolean> booleanGen = new BooleansDSL().all();

    public static Gen<Integer> dateGen = new IntegersDSL().all();

    public static Map<AbstractType<?>, Gen<?>> types = ImmutableMap.<AbstractType<?>, Gen<?>>builder()
                                                       .put(AsciiType.instance, asciiGen)
                                                       .put(LongType.instance, longGen)
//                                                       .put(BytesType.instance, blobGen)
//                                                       .put(BooleanType.instance, booleanGen)
//                                                       .put(SimpleDateType.instance, dateGen)
.build();

    private static Gen<AbstractType<?>> nativeTypeGenerator = fromValues(types.keySet());

    // TODO: use this
    private static Gen<ClusteringOrder> clusteringOrderGenerator = fromValues(new ClusteringOrder[]{ ClusteringOrder.DESC, ClusteringOrder.ASC });

    private static Gen<String> pkNameGenerator()
    {
        return monotonicGen().map(i -> String.format("pk" + i));
    }

    @SuppressWarnings("unchecked")
    public static Gen<ColumnSpec<?>> pkColumnGenerator()
    {
        return pkNameGenerator().zip(nativeTypeGenerator,
                                     (name, type) -> new ColumnSpec(name,
                                                                    type,
                                                                    ColumnMetadata.Kind.PARTITION_KEY));
    }

    private static Gen<String> ckNameGenerator()
    {
        return monotonicGen().map(i -> String.format("ck" + i));
    }

    public static Gen<String> tableNameGenerator = monotonicGen()
                                                    .map(i -> String.format("table" + i));


    @SuppressWarnings("unchecked")
    public static Gen<ColumnSpec<?>> ckColumnGenerator()
    {
        return ckNameGenerator().zip(nativeTypeGenerator.assuming(type -> type != DurationType.instance),
                                     clusteringOrderGenerator,
                                     (name, type, order) -> new ColumnSpec(name,
                                                                           order == ClusteringOrder.DESC ? ReversedType.getInstance(type) : type,
                                                                           ColumnMetadata.Kind.CLUSTERING));
    }

    private static Gen<String> staticNameGenerator()
    {
        return monotonicGen()
               .map(i -> String.format("s" + i));
    }

    public static Gen<ColumnSpec<?>> staticColumnGenerator()
    {
        return staticNameGenerator().zip(nativeTypeGenerator,
                                         (name, type) -> new ColumnSpec<>(name,
                                                                          type,
                                                                          ColumnMetadata.Kind.STATIC));
    }

    private static Gen<String> regularNameGenerator()
    {
        return monotonicGen().map(i -> String.format("regular" + i));
    }

    public static Gen<ColumnSpec<?>> regularColumnGenerator()
    {
        return regularNameGenerator().zip(nativeTypeGenerator,
                                          (name, type) -> new ColumnSpec<>(name,
                                                                           type,
                                                                           ColumnMetadata.Kind.REGULAR));
    }

    private static ListsDSL listsDSL = new ListsDSL();

    public static Gen<SchemaSpec> schemaDefinitionGen(String keyspace)
    {
        return tableNameGenerator.zip(listsDSL.of(pkColumnGenerator()).ofSizeBetween(1, 1),
                                      listsDSL.of(ckColumnGenerator()).ofSizeBetween(2, 3),
                                      listsDSL.of(staticColumnGenerator()).ofSizeBetween(1, 5),
                                      listsDSL.of(regularColumnGenerator()).ofSizeBetween(1, 5),
                                      (name, pks, clusterings, statics, regulars) -> new SchemaSpec(keyspace, name, pks, clusterings, statics, regulars))
                                 .assuming(schemaSpec -> schemaSpec.staticColumns.size() > 0 == schemaSpec.clusteringKeys.size() > 0);
    }

//    public static Gen<Pair<SchemaSpec, List<String>>> schemaAndDataGen(String keyspace)
//    {
//        return schemaDefinitionGen(keyspace).flatMap(schema -> {
//            return (RandomnessSource in) -> Pair.create(schema, SourceDSL.lists().of(writeGen(schema).map(Pair::right)).ofSizeBetween(100, 1000).generate(in));
//        });
//    }
}
