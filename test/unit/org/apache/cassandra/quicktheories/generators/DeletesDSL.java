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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.ArbitraryDSL;
import org.quicktheories.generators.Generate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.column;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;

public class DeletesDSL
{
    // TODO: delete a single field
    // TODO: conditional updates
    public enum DeleteType
    {
        SINGLE_PARTITION,
//        PARTITION_RANGE_OPEN,
//        PARTITION_RANGE_CLOSED,
//        MULTI_CLUSTERING_SLICE,
//        MULTI_CLUSTERING_RANGE,
        SINGLE_ROW,
        CLUSTERING_SLICE,
        CLUSTERING_RANGE
    }

    public DeletesBuilder partitionDelete(SchemaSpec schema)
    {
        return new DeletesBuilder(schema,
                                  schema.partitionKeyGenerator,
                                  null,
                                  DeleteType.SINGLE_PARTITION);
    }


    /**
     * Generate a single partition delete
     *
     * @param schema        the schemaSpec to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder partitionDelete(SchemaSpec schema,
                                          Gen<Object[]> partitionKeys)
    {
        return new DeletesBuilder(schema,
                                  partitionKeys,
                                  null,
                                  DeleteType.SINGLE_PARTITION);
    }

    public DeletesBuilder rowDelete(SchemaSpec schema)
    {
        return new DeletesBuilder(schema,
                                  schema.partitionKeyGenerator,
                                  (pk) -> schema.clusteringKeyGenerator,
                                  DeleteType.SINGLE_ROW);
    }

    /**
     * Generate a row delete within a partition.
     *
     * @param schema        the schemaSpec to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @param clusterings   a generator used when generating which rows/ranges within a partition to delete
     * @return a {@link DeletesBuilder} used to customize and create the generator
     */
    public DeletesBuilder rowDelete(SchemaSpec schema,
                                    Gen<Object[]> partitionKeys,
                                    Function<Object[], Gen<Object[]>> clusterings)
    {
        return new DeletesBuilder(schema,
                                  partitionKeys,
                                  clusterings,
                                  DeleteType.SINGLE_ROW);
    }

    public DeletesBuilder rowSliceDelete(SchemaSpec schema,
                                         Gen<Object[]> partitionKeys,
                                         Function<Object[], Gen<Object[]>> clusterings)
    {
        return new DeletesBuilder(schema,
                                  partitionKeys,
                                  clusterings,
                                  DeleteType.CLUSTERING_SLICE);
    }

    public DeletesBuilder rowRangeDelete(SchemaSpec schema,
                                         Gen<Object[]> partitionKeys,
                                         Function<Object[], Gen<Object[]>> clusterings)
    {
        return new DeletesBuilder(schema,
                                  partitionKeys,
                                  clusterings,
                                  DeleteType.CLUSTERING_RANGE);
    }

    public static class Relation
    {
        private final String column;
        private final Sign sign;
        private final Object value;

        public Relation(String column,
                        Sign sign,
                        Object value)
        {
            this.column = column;
            this.sign = sign;
            this.value = value;
        }

        public Clause toClause()
        {
            return sign.getClause(column, bindMarker());
        }
    }

    public static Relation relation(String column, Sign sign, Object value)
    {
        return new Relation(column, sign, value);
    }

    public static class DeletesBuilder
    {
        private final SchemaSpec schemaSpec;
        private final DeleteType deleteType;
        private final Gen<Object[]> pkGen;
        private final Function<Object[], Gen<Object[]>> ckGenSupplier;
        private Gen<Optional<List<String>>> columnDeleteGenerator = Generate.constant(Optional.empty());
        private Gen<Optional<Long>> timestampGen = Generate.constant(Optional.empty());

        DeletesBuilder(SchemaSpec schemaSpec,
                       Gen<Object[]> pkGen,
                       Function<Object[], Gen<Object[]>> ckGenSupplier,
                       DeleteType deleteType)
        {
            this.schemaSpec = schemaSpec;
            this.pkGen = pkGen;
            this.ckGenSupplier = ckGenSupplier;
            this.deleteType = deleteType;
        }

        public DeletesBuilder withTimestamp(Gen<Long> timestamps)
        {
            this.timestampGen = timestamps.map(Optional::of);
            return this;
        }

        public DeletesBuilder withTimestamp(long ts)
        {
            this.timestampGen = arbitrary().constant(Optional.of(ts));
            return this;
        }

        public DeletesBuilder withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }

        // TODO: add static columns?
        public DeletesBuilder deleteColumns()
        {
            this.columnDeleteGenerator = Extensions.subsetGenerator(schemaSpec.regularColumns.stream()
                                                                                             .map(ColumnSpec::name)
                                                                                             .collect(Collectors.toList()))
                                                   .map(Optional::of);
            return this;
        }
        private static void addRelation(Object[] pk, List<ColumnSpec<?>> columnSpecs, List<Relation> relations)
        {
            for (int i = 0; i < pk.length; i++)
            {
                ColumnSpec<?> spec = columnSpecs.get(i);
                relations.add(relation(spec.name, Sign.EQ, pk[i]));
            }
        }

        private static Gen<Sign> signGen = Generate.enumValues(Sign.class);

        // TODO: take this out; combine with reads dsl
        private Gen<List<Relation>> relationsGen()
        {
            return (prng) -> {
                List<Relation> relations = new ArrayList<>();
                switch (deleteType)
                {
                    case SINGLE_PARTITION:
                    {
                        addRelation(pkGen.generate(prng), schemaSpec.partitionKeys, relations);
                        break;
                    }
                    case SINGLE_ROW:
                    {
                        Object[] pk = pkGen.generate(prng);
                        addRelation(pk, schemaSpec.partitionKeys, relations);
                        Gen<Object[]> ckGen = ckGenSupplier.apply(pk);
                        addRelation(ckGen.generate(prng), schemaSpec.clusteringKeys, relations);
                        break;
                    }
                    case CLUSTERING_SLICE:
                    {
                        Object[] pk = pkGen.generate(prng);
                        addRelation(pk, schemaSpec.partitionKeys, relations);
                        Gen<Object[]> ckGen = ckGenSupplier.apply(pk);
                        Object[] ck = ckGen.generate(prng);
                        for (int i = 0; i < ck.length; i++)
                        {
                            ColumnSpec<?> spec = schemaSpec.clusteringKeys.get(i);
                            Sign sign = signGen.generate(prng);
                            relations.add(relation(spec.name, sign, ck[i]));

                            if (sign != Sign.EQ)
                                break;
                        }

                        break;
                    }
                    case CLUSTERING_RANGE:
                    {
                        Object[] pk = pkGen.generate(prng);
                        addRelation(pk, schemaSpec.partitionKeys, relations);
                        Gen<Object[]> ckGen = ckGenSupplier.apply(pk);
                        Object[] ck1 = ckGen.generate(prng);
                        Object[] ck2 = ckGen.generate(prng);
                        assert ck1.length == ck2.length;
                        for (int i = 0; i < ck1.length; i++)
                        {
                            ColumnSpec<?> spec = schemaSpec.clusteringKeys.get(i);
                            Sign sign = signGen.generate(prng);
                            relations.add(relation(spec.name, sign, ck1[i]));
                            // TODO (alexp): we can also use a roll of dice to mark inclusion/exclusion
                            if (sign.isNegatable())
                                relations.add(relation(spec.name, sign.negate(), ck2[i]));

                            if (sign != Sign.EQ)
                                break;
                        }
                    }
                }

                return relations;
            };
        }

        public Gen<Delete> build()
        {
            return columnDeleteGenerator.zip(relationsGen(),
                                             timestampGen,
                                             (columns, relations, ts) -> new Delete(schemaSpec,
                                                                                    columns,
                                                                                    relations,
                                                                                    ts));
        }
    }

    public static class Delete
    {
        private final SchemaSpec schemaSpec;
        private final Optional<List<String>> columnsToDelete;
        private final Optional<Long> timestamp;
        public final List<Relation> relations;

        Delete(SchemaSpec schemaSpec,
               Optional<List<String>> columnsToDelete,
               List<Relation> relations,
               Optional<Long> timestamp)
        {
            this.schemaSpec = schemaSpec;
            this.relations = relations;
            this.columnsToDelete = columnsToDelete;
            this.timestamp = timestamp;
        }

        public CompiledStatement compile()
        {
            com.datastax.driver.core.querybuilder.Delete delete = (columnsToDelete.isPresent() ?
                                                                   QueryBuilder.delete(toArray(columnsToDelete.get())) :
                                                                   QueryBuilder.delete())
                                                                  .from(schemaSpec.ksName, schemaSpec.tableName);
            com.datastax.driver.core.querybuilder.Delete.Where where = delete.where();

            Object[] bindings = new Object[relations.size()];
            for (int i = 0; i < relations.size(); i++)
            {
                where.and(relations.get(i).toClause());
                bindings[i] = relations.get(i).value;
            }

            timestamp.ifPresent(delete::setDefaultTimestamp);
            return new CompiledStatement(delete.toString(), bindings);
        }
    }

    public static String[] toArray(List<String> strings)
    {
        return strings.toArray(new String[strings.size()]);
    }
}

