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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.quicktheories.generators.SourceDSL.*;

public class WritesDSL
{
    /**
     * Generate a random write
     *
     * @param schema the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WriteBuilder<Insert> row(SchemaSpec schema)
    {
        return row(schema, schema.partitionKeyGenerator);
    }

    /**
     * Generate a write to the given partition (randomly generated row and values)
     *
     * @param schema       the schema to generate writes for
     * @param partitionKey the partition key to write to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WriteBuilder<Insert> row(SchemaSpec schema, Object[] partitionKey)
    {
        return row(schema, Generate.constant(partitionKey));
    }

    /**
     * Generate a random write
     *
     * @param schema        the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WriteBuilder<Insert> row(SchemaSpec schema, Gen<Object[]> partitionKeys)
    {
        return row(schema, partitionKeys, schema.clusteringKeyGenerator, schema.rowDataGenerator);
    }

    public WriteBuilder<Insert> row(SchemaSpec schema, Gen<Object[]> pkGen, Gen<Object[]> ckGen, Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen)
    {
        return new WriteBuilder<Insert>(schema, pkGen, ckGen, dataGen,
                                        Generate.constant(Optional::empty), Generate.constant(Optional::empty),
                                        WritesDSL::insertGen);
    }

    /**
     * Generate multiple writes to a single partition
     *
     * @param schemaSpec   the schema to generate writes for
     * @param partitionKey the partition to generates writes to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder<Insert> rows(SchemaSpec schemaSpec, Object[] partitionKey)
    {
        return rows(schemaSpec, Generate.constant(partitionKey));
    }

    /**
     * Generate writes to one or more partitions
     *
     * @param schemaSpec    the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder<Insert> rows(SchemaSpec schemaSpec, Gen<Object[]> partitionKeys)
    {
        return rows(schemaSpec, partitionKeys, schemaSpec.clusteringKeyGenerator, schemaSpec.rowDataGenerator);
    }

    /**
     * Generate writes to one or more partitions
     *
     * @param schemaSpec the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder<Insert> rows(SchemaSpec schemaSpec)
    {
        return rows(schemaSpec, schemaSpec.partitionKeyGenerator, schemaSpec.clusteringKeyGenerator, schemaSpec.rowDataGenerator);
    }

    public WritesBuilder<Insert> rows(SchemaSpec schema, Gen<Object[]> pkGen, Gen<Object[]> ckGen, Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen)
    {
        return new WritesBuilder<Insert>(schema, pkGen, ckGen, dataGen,
                                         Generate.constant(Optional::empty), Generate.constant(Optional::empty),
                                         1, 1,
                                         WritesDSL::insertGen);
    }

    public static abstract class AbstractBuilder<T extends AbstractBuilder>
    {
        protected final SchemaSpec schema;
        protected final Gen<Object[]> pkGen;
        protected final Gen<Object[]> ckGen;
        protected final Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen;
        protected final Gen<Optional<Long>> timestampGen;
        protected final Gen<Optional<Integer>> ttlGen;

        AbstractBuilder(SchemaSpec schema,
                        Gen<Object[]> pkGen,
                        Gen<Object[]> ckGen,
                        Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                        Gen<Optional<Long>> timestampGen,
                        Gen<Optional<Integer>> ttlGen)
        {
            this.schema = schema;
            this.pkGen = pkGen;
            this.ckGen = ckGen;
            this.dataGen = dataGen;
            this.timestampGen = timestampGen;
            this.ttlGen = ttlGen;
        }

        protected abstract T make(SchemaSpec schema,
                                  Gen<Object[]> pkGen,
                                  Gen<Object[]> ckGen,
                                  Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                  Gen<Optional<Long>> timestampGen,
                                  Gen<Optional<Integer>> ttlGen);

        public T withTimestamp(Gen<Long> timestamps)
        {
            return make(schema, pkGen, ckGen, dataGen,  timestamps.map(Optional::of), ttlGen);
        }

        public T withTimestamp(long ts)
        {
            return withTimestamp(arbitrary().constant(ts));
        }

        public T withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }

        public T withTTL(Gen<Integer> ttls)
        {
            return make(schema, pkGen, ckGen, dataGen, timestampGen, ttls.map(Optional::of));
        }

        public T withTTL(int ts)
        {
            return withTTL(arbitrary().constant(ts));
        }
    }

    public static class WriteBuilder<T extends Write> extends AbstractBuilder<WriteBuilder> implements Gen<T>
    {
        private final GeneratorBuilder<T> generatorBuilder;
        private final Gen<T> generator;

        WriteBuilder(SchemaSpec schema,
                     Gen<Object[]> pkGen,
                     Gen<Object[]> ckGen,
                     Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                     Gen<Optional<Long>> timestampGen,
                     Gen<Optional<Integer>> ttlGen,
                     GeneratorBuilder<T> generatorBuilder)
        {
            super(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen);
            this.generatorBuilder = generatorBuilder;
            this.generator = build(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, generatorBuilder);
        }

        protected WriteBuilder<T> make(SchemaSpec schema,
                                       Gen<Object[]> pkGen,
                                       Gen<Object[]> ckGen,
                                       Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                       Gen<Optional<Long>> timestampGen,
                                       Gen<Optional<Integer>> ttlGen)
        {
            return new WriteBuilder<T>(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, generatorBuilder);
        }

        public WriteBuilder<Insert> insert()
        {
            return new WriteBuilder<>(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, WritesDSL::insertGen);
        }

        public WriteBuilder<Update> update()
        {
            return new WriteBuilder<>(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, WritesDSL::updateGen);
        }

        private static <T extends Write> Gen<T> build(SchemaSpec schema,
                                                      Gen<Object[]> pkGen,
                                                      Gen<Object[]> ckGen,
                                                      Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                                      Gen<Optional<Long>> timestampGen,
                                                      Gen<Optional<Integer>> ttlGen,
                                                      GeneratorBuilder<T> gen)
        {
            return pkGen.zip(ckGen, FullKey::new)
                        .flatMap(fk -> gen.apply(schema, fk, dataGen, timestampGen, ttlGen));
        }

        public T generate(RandomnessSource prng)
        {
            return generator.generate(prng);
        }
    }

    public static class WritesBuilder<T extends Write> extends AbstractBuilder<WritesBuilder<T>> implements Gen<List<T>>
    {
        private final int minRows;
        private final int maxRows;
        private final GeneratorBuilder<T> generatorBuilder;
        private final Gen<List<T>> generator;

        WritesBuilder(SchemaSpec schema,
                              Gen<Object[]> pkGen,
                              Gen<Object[]> ckGen,
                              Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                              Gen<Optional<Long>> timestampGen,
                              Gen<Optional<Integer>> ttlGen,
                              int minRows,
                              int maxRows,
                              GeneratorBuilder<T> generatorBuilder)
        {
            super(schema, once(pkGen), ckGen, dataGen, timestampGen, ttlGen);
            this.minRows = minRows;
            this.maxRows = maxRows;
            this.generatorBuilder = generatorBuilder;
            this.generator = build(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, minRows, maxRows, generatorBuilder);
        }

        public WritesBuilder<T> rowCount(int count)
        {
            return rowCountBetween(count, count);
        }

        public WritesBuilder<T> rowCountBetween(int min, int max)
        {
            assert min > 0 : "Minimum row count should be non-negative but was " + min;
            assert min <= max : "Minimum row count not exceed maximum partition count";
            return new WritesBuilder<>(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, min, max, generatorBuilder);
        }

        public WritesBuilder<Insert> inserts()
        {
            return new WritesBuilder<>(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, minRows, maxRows, WritesDSL::insertGen);
        }

        public WritesBuilder<Update> updates()
        {
            return new WritesBuilder<>(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, minRows, maxRows, WritesDSL::updateGen);
        }

        private static <T extends Write> Gen<List<T>> build(SchemaSpec schema,
                                                            Gen<Object[]> pkGen,
                                                            Gen<Object[]> ckGen,
                                                            Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                                            Gen<Optional<Long>> timestampGen,
                                                            Gen<Optional<Integer>> ttlGen,
                                                            int minRows,
                                                            int maxRows,
                                                            GeneratorBuilder<T> gen)
        {
            Gen<T> rows = pkGen.zip(ckGen, FullKey::new)
                               .flatMap(fk -> gen.apply(schema, fk, dataGen, timestampGen, ttlGen));

            return lists().of(rows)
                          .ofSizeBetween(minRows, maxRows);
        }

        protected WritesBuilder<T> make(SchemaSpec schema,
                                        Gen<Object[]> pkGen,
                                        Gen<Object[]> ckGen,
                                        Gen<List<Pair<ColumnSpec<?>, Object>>> dataGen,
                                        Gen<Optional<Long>> timestampGen,
                                        Gen<Optional<Integer>> ttlGen)
        {
            return new WritesBuilder<T>(schema, pkGen, ckGen, dataGen, timestampGen, ttlGen, minRows, maxRows, generatorBuilder);
        }

        public List<T> generate(RandomnessSource prng)
        {
            return generator.generate(prng);
        }
    }

    /**
     * Auxilitary intermediate row representation
     */
    public static abstract class Write
    {
        protected final SchemaSpec schemaSpec;
        protected final FullKey fullKey;
        protected final List<Pair<ColumnSpec<?>, Object>> rowData;
        protected final Optional<Long> timestamp;
        protected final Optional<Integer> ttl;

        protected Write(SchemaSpec schemaSpec,
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

        @Override
        public String toString()
        {
            // A little wasteful, but necessary
            return compile().toString();
        }

        // TODO: insert with just a subset of columns
        // TODO: maybe we should have some sort of non-compiled statement interface
        public abstract CompiledStatement compile();
        public FullKey key()
        {
            return fullKey;
        }
    }

    public static class Insert extends Write
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

    public static class Update extends Write
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

            // TODO: will this work when only a subset of columns is set?
            for (Pair<ColumnSpec<?>, Object> row : rowData)
            {
                update.with(set(row.left.name, bindMarker()));
                bindings[bindingsCount++] = row.right;
            }

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

            timestamp.ifPresent(ts -> update.using(timestamp(ts)));
            ttl.ifPresent(ts -> update.using(ttl(ts)));

            return CompiledStatement.create(update.toString(), bindings);
        }
    }

    public interface GeneratorBuilder<T extends Write> extends Extensions.QuinFunction<SchemaSpec,
                                                                            FullKey,
                                                                            Gen<List<Pair<ColumnSpec<?>, Object>>>,
                                                                            Gen<Optional<Long>>,
                                                                            Gen<Optional<Integer>>,
                                                                            Gen<T>> {}
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

    public static <T> Gen<T> once(Gen<T> delegate)
    {
        return new Gen<T>()
        {
            private final AtomicReference<T> generated = new AtomicReference<>();
            public T generate(RandomnessSource prng)
            {
                return generated.updateAndGet(existing -> {
                    if (existing == null)
                        return delegate.generate(prng);
                    return existing;
                });
            }
        };
    }

}