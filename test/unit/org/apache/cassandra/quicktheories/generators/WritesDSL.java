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
import java.util.function.Supplier;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
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
     * @param schema the schema to generate writes for
     * @param partitionKey the partition key to write to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema, Object[] partitionKey)
    {
        return write(schema, Generate.constant(partitionKey));
    }

    /**
     * Generate a random write
     * @param schema the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema)
    {
        return write(schema, schema.partitionKeyGenerator);
    }


    /**
     * Generate a random write
     * @param schema the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public static WriteBuilder write(SchemaSpec schema, Gen<Object[]> partitionKeys)
    {
        return new WriteBuilder(schema, partitionKeys);
    }

    /**
     * Generate multiple writes to a single partition
     * @param schemaSpec the schema to generate writes for
     * @param partitionKey the partition to generates writes to
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec, Object[] partitionKey)
    {
        return new WritesBuilder(schemaSpec, Generate.constant(partitionKey));
    }

    /**
     * Generate writes to one or more partitions
     * @param schemaSpec the schema to generate writes for
     * @param partitionKeys the generator to use when generating partition keys
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec, Gen<Object[]> partitionKeys)
    {
        return new WritesBuilder(schemaSpec, partitionKeys);
    }

    /**
     * Generate writes to one or more partitions
     * @param schemaSpec the schema to generate writes for
     * @return a {@link WritesBuilder} used to customize and create the generator
     */
    public WritesBuilder writes(SchemaSpec schemaSpec)
    {
        return new WritesBuilder(schemaSpec);
    }


    /**
     * Generate a single partition delete
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder deletePartition(SchemaSpec schema,
                                          Gen<Object[]> partitionKeys)
    {
        return deletePartition(schema, () -> partitionKeys);
    }

    /**
     * Generate a single partition delete
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator supplier used when generating which partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder deletePartition(SchemaSpec schema,
                                          Supplier<Gen<Object[]>> partitionKeys)
    {
        return delete(schema, partitionKeys, () -> arbitrary().constant(new Object[]{})).partitionDeletesOnly();
    }


    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @param clusterings a generator used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Gen<Object[]> partitionKeys,
                                 Gen<Object[]> clusterings)
    {
        return delete(schema, () -> partitionKeys, () -> clusterings);
    }

    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator supplier used when generating which partition to delete
     * @param clusterings a generator used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Supplier<Gen<Object[]>> partitionKeys,
                                 Gen<Object[]> clusterings)
    {
        return delete(schema, partitionKeys, () -> clusterings);
    }

    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @param clusterings a generator supplier used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Gen<Object[]> partitionKeys,
                                 Supplier<Gen<Object[]>> clusterings)
    {
        return delete(schema, () -> partitionKeys, clusterings);
    }

    /**
     * Generate a delete within a partition. Deletes can be point or range deletes depending on further customization.
     *
     * @param schema the schema to generate the delete for
     * @param partitionKeys a generator supplier used when generating which partition to delete
     * @param clusterings a generator supplier used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder delete(SchemaSpec schema,
                                 Supplier<Gen<Object[]>> partitionKeys,
                                 Supplier<Gen<Object[]>> clusterings)
    {
        return new DeletesBuilder(schema, partitionKeys, clusterings);
    }


    public static class WriteBuilder {
        private final Gen<DataRow> rows;

        private WriteBuilder(SchemaSpec schema, Gen<Object[]> partitionKeys)
        {
            this(rowGenerator(schema, partitionKeys));
        }


        private WriteBuilder(Gen<DataRow> rows)
        {
            this.rows = rows;
        }

        public Gen<Pair<FullKey, Insert>> withTimestamp(Gen<Long> timestamps)
        {
            return this.rows.zip(timestamps, (r, ts) -> r.toInsert(ts));
        }

        public Gen<Pair<FullKey, Insert>> withTimestamp(long ts)
        {
            return withTimestamp(arbitrary().constant(ts));
        }


        public Gen<Pair<FullKey, Insert>> withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }
    }

    public static class WritesBuilder {
        private final SchemaSpec schema;
        private final Gen<Object[]> partitionKeys;

        private int minPartitions = 1;
        private int maxPartitions = 1;
        private int minRows = 1;
        private int maxRows = 1;

        private WritesBuilder(SchemaSpec schema)
        {
            this(schema, schema.partitionKeyGenerator);
        }


        private WritesBuilder(SchemaSpec schema, Gen<Object[]> partitionKeys)
        {
            this.schema = schema;
            this.partitionKeys = partitionKeys;
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

        public Gen<List<Pair<FullKey, Insert>>> withTimestamp(Gen<Long> timestamps)
        {

            Gen<List<Pair<FullKey, Insert>>> singlePartitionManyRows = partitionKeys.flatMap(pk ->
                                                                                lists().of(rowGenerator(schema, pk).zip(timestamps, (r, ts) -> r.toInsert(ts)))
                                                                                       .ofSizeBetween(minRows, maxRows));

            return lists().of(singlePartitionManyRows)
                          .ofSizeBetween(minPartitions, maxPartitions)
                          .map(writes -> writes.stream().reduce(new ArrayList<>(), (a, b) -> { a.addAll(b); return a; } ));

        }

        public Gen<List<Pair<FullKey, Insert>>> withTimestamp(long ts)
        {
            return withTimestamp(arbitrary().constant(ts));
        }


        public Gen<List<Pair<FullKey, Insert>>> withCurrentTimestamp()
        {
            return withTimestamp(in -> FBUtilities.timestampMicros());
        }

    }

    /**
     * Auxilitary intermediate row representation
     */
    private static class DataRow
    {
        private final SchemaSpec schemaSpec;
        private final FullKey fullKey;
        private final List<Pair<ColumnSpec<?>, Object>> rowData;

        public DataRow(SchemaSpec schemaSpec,
                       FullKey fullKey,
                       List<Pair<ColumnSpec<?>, Object>> rowData)
        {
            this.schemaSpec = schemaSpec;
            this.fullKey = fullKey;
            this.rowData = rowData;
        }

        public Pair<FullKey, Insert> toInsert(long ts)
        {
            Insert insert = QueryBuilder.insertInto(schemaSpec.ksName, schemaSpec.tableName);

            for (int i = 0; i < schemaSpec.partitionKeys.size(); i++)
            {
                insert.value(schemaSpec.partitionKeys.get(i).name,
                             fullKey.partition[i]);
            }

            for (int i = 0; i < schemaSpec.clusteringKeys.size(); i++)
            {
                insert.value(schemaSpec.clusteringKeys.get(i).name,
                             fullKey.clustering[i]);
            }


            for (Pair<ColumnSpec<?>, Object> row : rowData)
            {
                insert.value(row.left.name,
                             row.right);
            }

            insert.using(timestamp(ts));
            return Pair.create(fullKey, insert);
        }

    }

    public static class DeletesBuilder
    {
        private final SchemaSpec schema;
        private final Supplier<Gen<Object[]>> partitionKeys;
        private final Supplier<Gen<Object[]>> clusterings;

        private boolean rowPointDeletes = true;
        private boolean rowRangeDeletes = true;


        public DeletesBuilder(SchemaSpec schema, Supplier<Gen<Object[]>> partitionKeys, Supplier<Gen<Object[]>> clusterings)
        {
            this.schema = schema;
            this.partitionKeys = partitionKeys;
            this.clusterings = clusterings;
        }

        public DeletesBuilder partitionDeletesOnly()
        {
            rowRangeDeletes = false;
            rowPointDeletes = false;
            return this;
        }

        public DeletesBuilder pointDeletesOnly()
        {
            rowRangeDeletes = false;
            rowPointDeletes = true;
            return this;
        }

        public DeletesBuilder rangeDeletesOnly()
        {
            rowPointDeletes = false;
            rowRangeDeletes = true;
            return this;
        }

        public DeletesBuilder pointOrRangeDeletes()
        {
            rowPointDeletes = rowRangeDeletes = true;
            return this;
        }


        public Gen<Delete> withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }

        // TODO (jwest): add version that takes timestamp generator
        // TODO (jwest): borrowed heavily from QueryGen.deleteGen with a few minor changes
        public Gen<Delete> withTimestamp(long ts)
        {
            return in -> {
                Gen<Object[]> clusteringGen = clusterings.get();
                Object[] pk = partitionKeys.get().generate(in);
                Object[] ckEq = clusteringGen.generate(in);

                Delete delete = QueryBuilder.delete().from(schema.ksName, schema.tableName);

                Delete.Where where = delete.where();

                for (int i = 0; i < schema.partitionKeys.size(); i++)
                {
                    ColumnSpec<?> pkColumn = schema.partitionKeys.get(i);
                    where.and(eq(pkColumn.name, pk[i]));
                }

                // TODO (jwest): randomly generate point deletes too
                if (schema.clusteringKeys.size() == 0 || !rowRangeDeletes && !rowPointDeletes)
                {
                    delete.using(timestamp(ts));
                    return delete;
                }

                int eqs;
                if (rowRangeDeletes && !rowPointDeletes) // range only
                    eqs = integers().between(0, schema.clusteringKeys.size() - 1).generate(in);
                else if (rowPointDeletes && !rowRangeDeletes) // point only
                    eqs = schema.clusteringKeys.size();
                else // both
                    eqs = integers().between(0, schema.clusteringKeys.size()).generate(in);


                for (int i = 0; i < eqs; i++)
                {
                    ColumnSpec<?> ck = schema.clusteringKeys.get(i);
                    where.and(eq(ck.name, ckEq[i]));
                }

                // maybe include bound if range query
                int i = Math.max(eqs, 0);
                boolean includeBound = booleans().all().generate(in);
                if (includeBound && i < schema.clusteringKeys.size())
                {
                    Sign sign = arbitrary().pick(Sign.LT, Sign.GT, Sign.GTE, Sign.LTE).generate(in);
                    ColumnSpec<?> ck = schema.clusteringKeys.get(i);
                    where.and(sign.getClause(ck.name, ckEq[i]));

                    if (booleans().all().generate(in)) // maybe add opposite bound
                    {
                        Object[] ckBound = clusteringGen.generate(in);
                        where.and(sign.negate().getClause(ck.name, ckBound[i]));
                    }
                }

                delete.using(timestamp(ts));
                return delete;
            };
        }

    }

    private static Gen<DataRow> rowGenerator(SchemaSpec schema, Gen<Object[]> partitionKeys) {
        return partitionKeys.zip(schema.clusteringKeyGenerator, schema.rowDataGenerator,
                                 (pk, ck, data) -> new DataRow(schema, new FullKey(pk, ck), data));
    }


    private static Gen<DataRow> rowGenerator(SchemaSpec schema, Object[] pk) {
        return schema.clusteringKeyGenerator.zip(schema.rowDataGenerator,
                                                 (ck, data) -> new DataRow(schema, new FullKey(pk, ck), data));
    }

}
