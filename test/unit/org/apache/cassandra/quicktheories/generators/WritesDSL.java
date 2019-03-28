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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.quicktheories.generators.SourceDSL.*;

public class WritesDSL
{

    /**
     * Generate a write to the given partition (randomly generated row and values)
     *
     * @param schema       the schema to generate writes for
     * @param partitionKey the partition key to write to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema, Object[] partitionKey)
    {
        return write(schema, Generate.constant(partitionKey));
    }

    /**
     * Generate a random write
     *
     * @param schema the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema)
    {
        return write(schema, schema.partitionKeyGenerator);
    }


    /**
     * Generate a random write
     *
     * @param schema        the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema, Gen<Object[]> partitionKeys)
    {
        return new WriteBuilder(schema, partitionKeys, schema.clusteringKeyGenerator, schema.rowDataGenerator);
    }

    /**
     * Generate multiple writes to a single partition
     *
     * @param schemaSpec   the schema to generate writes for
     * @param partitionKey the partition to generates writes to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec, Object[] partitionKey)
    {
        return new WritesBuilder(schemaSpec, Generate.constant(partitionKey), schemaSpec.clusteringKeyGenerator, schemaSpec.rowDataGenerator);
    }

    /**
     * Generate writes to one or more partitions
     *
     * @param schemaSpec    the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec, Gen<Object[]> partitionKeys)
    {
        return new WritesBuilder(schemaSpec, partitionKeys, schemaSpec.clusteringKeyGenerator, schemaSpec.rowDataGenerator);
    }

    /**
     * Generate writes to one or more partitions
     *
     * @param schemaSpec the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec)
    {
        return new WritesBuilder(schemaSpec,
                                 schemaSpec.partitionKeyGenerator,
                                 schemaSpec.clusteringKeyGenerator,
                                 schemaSpec.rowDataGenerator);
    }


    public static class WriteBuilder
    {
        private final SchemaSpec schema;
        private final Gen<Object[]> pkGen;
        private final Gen<Object[]> ckGen;
        private final Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen;
        private Gen<Optional<Long>> timestampGen = Generate.constant(Optional::empty);
        private Gen<Optional<Integer>> ttlGen = Generate.constant(Optional::empty);

        WriteBuilder(SchemaSpec schema,
                     Gen<Object[]> pkGen,
                     Gen<Object[]> ckGen,
                     Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen)
        {
            this.schema = schema;
            this.pkGen = pkGen;
            this.ckGen = ckGen;
            this.dataGen = dataGen;
        }

        public WriteBuilder withTimestamp(Gen<Long> timestamps)
        {
            this.timestampGen = timestamps.map(Optional::of);
            return this;
        }

        public WriteBuilder withTimestamp(long ts)
        {
            this.timestampGen = arbitrary().constant(Optional.of(ts));
            return this;
        }

        public WriteBuilder withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }

        public WriteBuilder withTTL(Gen<Integer> timestamps)
        {
            this.ttlGen = timestamps.map(Optional::of);
            return this;
        }

        public WriteBuilder withTTL(int ts)
        {
            this.ttlGen = arbitrary().constant(Optional.of(ts));
            return this;
        }

        private Gen<Insert> insert()
        {
            return build(WritesDSL::insertGen);
        }

        private Gen<Update> updates()
        {
            return build(WritesDSL::updateGen);
        }

        private <T extends DataRow> Gen<T> build(Extensions.QuinFunction<SchemaSpec,
                                                                        FullKey,
                                                                        Gen<List<Pair<ColumnSpec<?>, Object>>>,
                                                                        Gen<Optional<Long>>,
                                                                        Gen<Optional<Integer>>,
                                                                        Gen<T>> gen)
        {
            return pkGen.zip(ckGen, FullKey::new)
                        .flatMap(fk -> gen.apply(schema, fk, dataGen, timestampGen, ttlGen));
        }


    }

    public static class WritesBuilder
    {
        private final SchemaSpec schema;
        private final Gen<Object[]> pkGen;
        private final Gen<Object[]> ckGen;
        private final Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen;
        private Gen<Optional<Long>> timestampGen = Generate.constant(Optional.empty());
        private Gen<Optional<Integer>> ttlGen = Generate.constant(Optional.empty());

        private int minPartitions = 1;
        private int maxPartitions = 1;
        private int minRows = 1;
        private int maxRows = 1;

        private WritesBuilder(SchemaSpec schema,
                              Gen<Object[]> pkGen,
                              Gen<Object[]> ckGen,
                              Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen)
        {
            this.schema = schema;
            this.pkGen = pkGen;
            this.ckGen = ckGen;
            this.dataGen = dataGen;
        }

        public WritesBuilder partitionCount(int count)
        {
            return partitionCountBetween(count, count);
        }

        public WritesBuilder partitionCountBetween(int min, int max)
        {
            assert min > 0 : "Minimum partition count should be non-negative but was " + min;
            assert min <= max : "Minimum partition count not exceed maximum partition count";

            minPartitions = min;
            maxPartitions = max;
            return this;
        }

        public WritesBuilder rowCount(int count)
        {
            return rowCountBetween(count, count);
        }

        public WritesBuilder rowCountBetween(int min, int max)
        {
            assert min > 0 : "Minimum row count should be non-negative but was " + min;
            assert min <= max : "Minimum row count not exceed maximum partition count";

            minRows = min;
            maxRows = max;
            return this;
        }

        public WritesBuilder withTimestamp(Gen<Long> timestamps)
        {
            this.timestampGen = timestamps.map(Optional::of);
            return this;
        }

        public WritesBuilder withTimestamp(long ts)
        {
            this.timestampGen = arbitrary().constant(Optional.of(ts));
            return this;
        }

        public WritesBuilder withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }

        public WritesBuilder withTTL(Gen<Integer> ttls)
        {
            this.ttlGen = ttls.map(Optional::of);
            return this;
        }

        public Gen<List<Insert>> inserts()
        {
            return build(WritesDSL::insertGen);
        }

        public Gen<List<Update>> updates()
        {
            return build(WritesDSL::updateGen);
        }

        private <T extends DataRow> Gen<List<T>> build(Extensions.QuinFunction<SchemaSpec,
                                                                              FullKey,
                                                                              Gen<List<Pair<ColumnSpec<?>, Object>>>,
                                                                              Gen<Optional<Long>>,
                                                                              Gen<Optional<Integer>>,
                                                                              Gen<T>> gen)
        {
            Gen<List<T>> rows =
            pkGen.flatMap(pk -> {
                return lists().of(ckGen.flatMap(ck -> gen.apply(schema, new FullKey(pk, ck), dataGen, timestampGen, ttlGen)))
                              .ofSizeBetween(minRows, maxRows);
            });

            return lists().of(rows)
                          .ofSizeBetween(minPartitions, maxPartitions)
                          .map(WritesDSL::flatten);
        }
    }

    private static <T> List<T> flatten(List<List<T>> llist)
    {
        return llist.stream().reduce(new ArrayList<>(), (a, b) -> {
            a.addAll(b);
            return a;
        });
    }


    /**
     * Auxilitary intermediate row representation
     */
    public static abstract class DataRow
    {
        protected final SchemaSpec schemaSpec;
        protected final FullKey fullKey;
        protected final List<Pair<ColumnSpec<?>, Object>> rowData;
        protected final Optional<Long> timestamp;
        protected final Optional<Integer> ttl;

        protected DataRow(SchemaSpec schemaSpec,
                          FullKey fullKey,
                          List<Pair<ColumnSpec<?>, Object>> rowData,
                          Optional<Long> timestamp,
                          Optional<Integer> ttl)
        {
            this.schemaSpec = schemaSpec;
            this.fullKey = fullKey;
            this.rowData = rowData;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }

        // TODO: insert with just a subset of columns
        public abstract CompiledStatement compile();
        public FullKey key()
        {
            return fullKey;
        }
    }

    public static class Insert extends DataRow
    {
        Insert(SchemaSpec schemaSpec,
               FullKey fullKey,
               List<Pair<ColumnSpec<?>, Object>> rowData,
               Optional<Long> timestamp,
               Optional<Integer> ttl)
        {
            super(schemaSpec, fullKey, rowData, timestamp, ttl);
        }

        // TODO: insert and update with just a subset of columns
        public CompiledStatement compile()
        {
            Object[] bindings = new Object[schemaSpec.allColumns.size()];
            int bindingsCount = 0;
            com.datastax.driver.core.querybuilder.Insert insert = insertInto(schemaSpec.ksName, schemaSpec.tableName);

            for (int i = 0; i < schemaSpec.partitionKeys.size(); i++)
            {
                insert.value(schemaSpec.partitionKeys.get(i).name,
                             bindMarker());
                bindings[bindingsCount++] = fullKey.partition[i];
            }

            for (int i = 0; i < schemaSpec.clusteringKeys.size(); i++)
            {
                insert.value(schemaSpec.clusteringKeys.get(i).name,
                             bindMarker());
                bindings[bindingsCount++] = fullKey.clustering[i];
            }


            for (Pair<ColumnSpec<?>, Object> row : rowData)
            {
                insert.value(row.left.name,
                             bindMarker());
                bindings[bindingsCount++] = row.right;
            }

            timestamp.ifPresent(ts -> insert.using(timestamp(ts)));
            ttl.ifPresent(ts -> insert.using(ttl(ts)));

            return CompiledStatement.create(insert.toString(), bindings);
        }
    }

    public static class Update extends DataRow
    {
        Update(SchemaSpec schemaSpec,
               FullKey fullKey,
               List<Pair<ColumnSpec<?>, Object>> rowData,
               Optional<Long> timestamp,
               Optional<Integer> ttl)
        {
            super(schemaSpec, fullKey, rowData, timestamp, ttl);
        }

        // TODO: insert with just a subset of columns
        public CompiledStatement compile()
        {
            Object[] bindings = new Object[schemaSpec.allColumns.size()];
            int bindingsCount = 0;
            com.datastax.driver.core.querybuilder.Update update = update(schemaSpec.ksName, schemaSpec.tableName);

            com.datastax.driver.core.querybuilder.Update.Where where = update.where();
            for (int i = 0; i < schemaSpec.partitionKeys.size(); i++)
            {
                where.and(eq(schemaSpec.partitionKeys.get(i).name, bindMarker()));
                bindings[bindingsCount++] = fullKey.partition[i];
            }

            for (int i = 0; i < schemaSpec.clusteringKeys.size(); i++)
            {
                where.and(eq(schemaSpec.clusteringKeys.get(i).name, bindMarker()));
                bindings[bindingsCount++] = fullKey.clustering[i];
            }

            for (Pair<ColumnSpec<?>, Object> row : rowData)
            {
                update.with(set(row.left.name, bindMarker()));
                bindings[bindingsCount++] = row.right;
            }

            timestamp.ifPresent(ts -> update.using(timestamp(ts)));
            ttl.ifPresent(ts -> update.using(ttl(ts)));

            return CompiledStatement.create(update.toString(), bindings);
        }
    }

    private static Gen<Insert> insertGen(SchemaSpec schema,
                                          Gen<Object[]> pkGen, Gen<Object[]> ckGen, Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                          Gen<Optional<Long>> tsGen, Gen<Optional<Integer>> ttlGen)
    {
        return pkGen.zip(ckGen, FullKey::new)
                    .flatMap(fk -> insertGen(schema, fk, dataGen, tsGen, ttlGen));
    }

    private static Gen<Update> updateGen(SchemaSpec schema,
                                          Gen<Object[]> pkGen, Gen<Object[]> ckGen, Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                          Gen<Optional<Long>> tsGen, Gen<Optional<Integer>> ttlGen)
    {
        return pkGen.zip(ckGen, FullKey::new)
                    .flatMap(fk -> updateGen(schema, fk, dataGen, tsGen, ttlGen));
    }

    private static Gen<Insert> insertGen(SchemaSpec schema,
                                         FullKey fk, Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                         Gen<Optional<Long>> tsGen, Gen<Optional<Integer>> ttlGen)
    {
        return dataGen.zip(tsGen, ttlGen,
                           (data, ts, ttl) -> new Insert(schema, fk, data, ts, ttl));
    }

    private static Gen<Update> updateGen(SchemaSpec schema,
                                          FullKey fk, Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                          Gen<Optional<Long>> tsGen, Gen<Optional<Integer>> ttlGen)
    {
        return dataGen.zip(tsGen, ttlGen,
                           (data, ts, ttl) -> new Update(schema, fk, data, ts, ttl));
    }
}