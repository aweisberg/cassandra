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
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.ClusteringOrder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;

import static org.apache.cassandra.quicktheories.generators.Extensions.monotonicGen;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.characters;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;
import static org.quicktheories.generators.SourceDSL.longs;

public class SchemaDSL
{
    public Builder keyspace(String keyspace) {
        return new Builder(keyspace);
    }

    @SuppressWarnings("unchecked")
    public static <T> Gen<T> fromValues(Collection<T> allValues)
    {
        return fromValues((T[]) allValues.toArray());
    }

    public static <T> Gen<T> fromValues(T[] allValues)
    {
        return integers().between(0, allValues.length - 1)
                        .map(i -> allValues[i]);
    }

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

    public static Map<AbstractType<?>, Gen<?>> types = ImmutableMap.<AbstractType<?>, Gen<?>>builder()
                                                       .put(AsciiType.instance, asciiGen)
                                                       .put(LongType.instance, longGen)
//                                                       . put(BytesType.instance, blobGen)
//                                                       .put(BooleanType.instance, booleanGen)
//                                                       .put(SimpleDateType.instance, dateGen)
                                                       .build();

    private static Gen<AbstractType<?>> nativeTypeGenerator = fromValues(types.keySet());

    private static Gen<ClusteringOrder> clusteringOrderGenerator = Generate.enumValues(ClusteringOrder.class);

    private static Gen<String> pkNameGenerator()
    {
        return Extensions.monotonicGen().map(i -> String.format("pk" + i));
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
        return Extensions.monotonicGen().map(i -> String.format("ck" + i));
    }

    public static Gen<String> tableNameGenerator = Extensions.monotonicGen()
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

    public static class Builder
    {
        private final String keyspace;

        private Gen<String> tableNameGen;
        private int minPks = 1;
        private int maxPks = 1;
        private int minStatics = 0;
        private int maxStatics = 0;
        private int minCluster = 0;
        private int maxCluster = 0;
        private int minRegular = 0;
        private int maxRegular = 0;

        public Builder(String keyspace)
        {
            this(keyspace, tableNameGenerator);
        }

        public Builder(String keyspace, Gen<String> tableNameGen)
        {
            this.keyspace = keyspace;
            this.tableNameGen = tableNameGen;
        }

        public Builder partitionKeyColumnCount(int numCols)
        {
            return partitionKeyColumnCount(numCols, numCols);
        }

        public Builder partitionKeyColumnCount(int minCols, int maxCols)
        {
            this.minPks = minCols;
            this.maxPks = maxCols;
            return this;
        }

        public Builder clusteringColumnCount(int numCols)
        {
            return clusteringColumnCount(numCols, numCols);
        }

        public Builder clusteringColumnCount(int minCols, int maxCols)
        {
            this.minCluster = minCols;
            this.maxCluster = maxCols;
            return this;
        }

        public Builder staticColumnCount(int numCols)
        {
            return staticColumnCount(numCols, numCols);
        }

        public Builder staticColumnCount(int minCols, int maxCols)
        {
            this.minStatics = minCols;
            this.maxStatics = maxCols;
            return this;
        }

        public Builder regularColumnCount(int numCols)
        {
            return regularColumnCount(numCols, numCols);
        }

        public Builder regularColumnCount(int minCols, int maxCols)
        {
            this.minRegular = minCols;
            this.maxRegular = maxCols;
            return this;
        }

        public Gen<SchemaSpec> build()
        {
            return tableNameGen.zip(lists().of(pkColumnGenerator()).ofSizeBetween(minPks, maxPks),
                                    lists().of(ckColumnGenerator()).ofSizeBetween(minCluster, maxCluster),
                                    lists().of(staticColumnGenerator()).ofSizeBetween(minStatics, maxStatics),
                                    lists().of(regularColumnGenerator()).ofSizeBetween(minRegular, maxRegular),
                                    (name, pks, cks, statics, regular) -> new SchemaSpec(keyspace, name, pks, cks, statics, regular))
                               .assuming(schemaSpec -> schemaSpec.staticColumns.size() == 0 || schemaSpec.clusteringKeys.size() > 0);
        }
    }
}
