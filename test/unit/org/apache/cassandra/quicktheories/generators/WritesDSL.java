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
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static java.util.stream.Collectors.toList;
import static org.quicktheories.generators.SourceDSL.*;


public class WritesDSL
{
    /**
     * Generate single row write
     */
    public static Gen<Pair<FullKey, Insert>> write(SchemaSpec schema, Object[] partitionKey)
    {
        return rowGenerator(schema,
                            Generate.constant(partitionKey),
                            schema.clusteringKeyGenerator)
               .map(DataRow::toInsert);
    }

    public static Gen<Pair<FullKey, Insert>> write(SchemaSpec schema, Gen<Object[]> pkGen)
    {
        return rowGenerator(schema,
                            pkGen,
                            schema.clusteringKeyGenerator)
               .map(DataRow::toInsert);
    }

    /**
     * Generate writes to multiple partitions
     */
    public Gen<List<Pair<FullKey, Insert>>> writes(SchemaSpec schemaSpec,
                                                   int maxPartitions,
                                                   int minPartitionSize,
                                                   int maxPartitionSize)
    {
        return SourceDSL.lists()
                        .of(WritesDSL.writes(schemaSpec, minPartitionSize, maxPartitionSize))
                        .ofSizeBetween(1, maxPartitions)
                        .map((List<List<Pair<FullKey, Insert>>> lists) -> {
                            List<Pair<FullKey, Insert>> rows = new ArrayList<>();
                            for (List<Pair<FullKey, Insert>> list : lists)
                            {
                                rows.addAll(list);
                            }
                            return rows;
                        });
    }

    /**
     * Generate writes to single partition
     */
    public static Gen<List<Pair<FullKey, Insert>>> writes(SchemaSpec schemaSpec,
                                                          int minPartitionSize,
                                                          int maxPartitionSize)
    {
        return rowsGenerator(schemaSpec,
                             schemaSpec.partitionKeyGenerator,
                             schemaSpec.clusteringKeyGenerator,
                             minPartitionSize,
                             maxPartitionSize)
               .map((rows) -> rows.stream().map(DataRow::toInsert).collect(toList()));
    }

    /**
     * Generate writes to single partition with predetermined partition key
     */
    public static Gen<List<Pair<FullKey, Insert>>> writes(SchemaSpec schemaSpec,
                                                          Object[] pk,
                                                          int minPartitionSize,
                                                          int maxPartitionSize)
    {
        return rowsGenerator(schemaSpec,
                             Generate.constant(() -> pk),
                             schemaSpec.clusteringKeyGenerator,
                             minPartitionSize,
                             maxPartitionSize)
               .map((rows) -> rows.stream().map(DataRow::toInsert).collect(toList()));
    }

    private static Gen<List<DataRow>> rowsGenerator(SchemaSpec schemaSpec,
                                                    Gen<Object[]> partitionKeyGenerator,
                                                    Gen<Object[]> clusteringKeyGenerator,
                                                    int minPartitionSize, int maxPartitionSize)
    {
        assert minPartitionSize > 0 : "Minimum partition size should be non-negative but was " + minPartitionSize;
        assert minPartitionSize <= maxPartitionSize : "Minimum partition should not exceed maximum partition size";

        return Extensions.combine(SourceDSL.integers().between(minPartitionSize, maxPartitionSize),
                                  rowGenerator(schemaSpec,
                                               partitionKeyGenerator,
                                               clusteringKeyGenerator),
                                  (prng, sizeGen, dataGen) -> {
                                      int size = sizeGen.generate(prng);

                                      List<DataRow> rows = new ArrayList<>(size);
                                      for (int i = 0; i < size; i++)
                                          rows.add(dataGen.generate(prng));

                                      return rows;
                                  });
    }

    /**
     * Provides a generator for data for a single partition
     */
    private static Gen<DataRow> rowGenerator(SchemaSpec schemaSpec,
                                            Gen<Object[]> partitionKeyGenerator,
                                            Gen<Object[]> clusteringKeyGenerator)
    {
        return Extensions.combine(partitionKeyGenerator,
                                  clusteringKeyGenerator,
                                  schemaSpec.rowDataGenerator,
                                  (prng, pkGen, ckGen, rowDataGen) -> {
                                      Object[] pk = pkGen.generate(prng);

                                      return new DataRow(schemaSpec,
                                                         new FullKey(pk, ckGen.generate(prng)),
                                                         rowDataGen.generate(prng));
                                  });
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

        public Pair<FullKey, Insert> toInsert()
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

            // TODO (alexp): make it possible to supply timestamp
            insert.using(timestamp(FBUtilities.timestampMicros()));
            return Pair.create(fullKey, insert);
        }

    }



    public DeletesBuilder deletePartition(SchemaSpec schema,
                                          Gen<Object[]> partitionKeys)
    {
        return deletePartition(schema, () -> partitionKeys);
    }

    public DeletesBuilder deletePartition(SchemaSpec schema,
                                          Supplier<Gen<Object[]>> partitionKeys)
    {
        return delete(schema, partitionKeys, () -> arbitrary().constant(new Object[]{})).partitionDeletesOnly();
    }


    public DeletesBuilder delete(SchemaSpec schema,
                                 Gen<Object[]> partitionKeys,
                                 Gen<Object[]> clusterings)
    {
        return delete(schema, () -> partitionKeys, () -> clusterings);
    }

    public DeletesBuilder delete(SchemaSpec schema,
                                 Supplier<Gen<Object[]>> partitionKeys,
                                 Gen<Object[]> clusterings)
    {
        return delete(schema, partitionKeys, () -> clusterings);
    }

    public DeletesBuilder delete(SchemaSpec schema,
                                 Gen<Object[]> partitionKeys,
                                 Supplier<Gen<Object[]>> clusterings)
    {
        return delete(schema, () -> partitionKeys, clusterings);
    }

    public DeletesBuilder delete(SchemaSpec schema,
                                 Supplier<Gen<Object[]>> partitionKeys,
                                 Supplier<Gen<Object[]>> clusterings)
    {
        return new DeletesBuilder(schema, partitionKeys, clusterings);
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

}
