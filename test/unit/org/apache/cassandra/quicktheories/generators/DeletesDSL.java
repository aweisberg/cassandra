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
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.utils.FBUtilities;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;

import static org.apache.cassandra.quicktheories.generators.Relation.QueryKind;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class DeletesDSL
{

    public Builder anyDelete(SchemaSpec schema,
                             Gen<Object[]> pkGen,
                             Function<Object[], Gen<Object[]>> ckGenSupplier)
    {
        return new Builder(schema,
                           pkGen,
                           ckGenSupplier,
                           Generate.enumValues(QueryKind.class));
    }

    public Builder partitionDelete(SchemaSpec schema)
    {
        return new Builder(schema,
                           schema.partitionKeyGenerator,
                           null,
                           Generate.constant(QueryKind.SINGLE_PARTITION));
    }


    /**
     * Generate a single partition delete
     *
     * @param schema        the schemaSpec to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @return a {@link Builder} used to customize and create the generator
     */
    public Builder partitionDelete(SchemaSpec schema,
                                   Gen<Object[]> partitionKeys)
    {
        return new Builder(schema,
                           partitionKeys,
                           null,
                           Generate.constant(QueryKind.SINGLE_PARTITION));
    }

    public Builder rowDelete(SchemaSpec schema)
    {
        return new Builder(schema,
                           schema.partitionKeyGenerator,
                           (pk) -> schema.clusteringKeyGenerator,
                           Generate.constant(QueryKind.SINGLE_ROW));
    }

    /**
     * Generate a row delete within a partition.
     *
     * @param schema        the schemaSpec to generate the delete for
     * @param partitionKeys a generator used when generating which partition to delete
     * @param clusterings   a generator used when generating which rows/ranges within a partition to delete
     * @return a {@link Builder} used to customize and create the generator
     */
    public Builder rowDelete(SchemaSpec schema,
                             Gen<Object[]> partitionKeys,
                             Function<Object[], Gen<Object[]>> clusterings)
    {
        return new Builder(schema,
                           partitionKeys,
                           clusterings,
                           Generate.constant(QueryKind.SINGLE_ROW));
    }

    public Builder rowSliceDelete(SchemaSpec schema)
    {
        return rowSliceDelete(schema,
                              schema.partitionKeyGenerator,
                              pk -> schema.clusteringKeyGenerator);
    }

    public Builder rowSliceDelete(SchemaSpec schema,
                                  Gen<Object[]> partitionKeys,
                                  Function<Object[], Gen<Object[]>> clusterings)
    {
        return new Builder(schema,
                           partitionKeys,
                           clusterings,
                           Generate.constant(QueryKind.CLUSTERING_SLICE));
    }

    public Builder rowRangeDelete(SchemaSpec schema)
    {
        return rowRangeDelete(schema,
                              schema.partitionKeyGenerator,
                              pk -> schema.clusteringKeyGenerator);
    }

    public Builder rowRangeDelete(SchemaSpec schema,
                                  Gen<Object[]> partitionKeys,
                                  Function<Object[], Gen<Object[]>> clusterings)
    {
        return new Builder(schema,
                           partitionKeys,
                           clusterings,
                           Generate.constant(QueryKind.CLUSTERING_RANGE));
    }

    public static class Builder implements Gen<Delete>
    {
        private final SchemaSpec schemaSpec;
        private final Gen<QueryKind> deleteKindGen;
        private final Gen<Object[]> pkGen;
        private final Function<Object[], Gen<Object[]>> ckGenSupplier;
        private final Gen<Optional<List<String>>> columnDeleteGenerator;
        private final Gen<Optional<Long>> timestampGen;
        private final Gen<Delete> generator;

        Builder(SchemaSpec schemaSpec,
                Gen<Object[]> pkGen,
                Function<Object[], Gen<Object[]>> ckGenSupplier,
                Gen<QueryKind> deleteKindGen)
        {
            this(schemaSpec, pkGen, ckGenSupplier, deleteKindGen,
                 Generate.constant(Optional.empty()),
                 Generate.constant(Optional.empty()));
        }

        Builder(SchemaSpec schemaSpec,
                Gen<Object[]> pkGen,
                Function<Object[], Gen<Object[]>> ckGenSupplier,
                Gen<QueryKind> deleteKindGen,
                Gen<Optional<List<String>>> columnDeleteGenerator,
                Gen<Optional<Long>> timestampGen)
        {
            this.schemaSpec = schemaSpec;
            this.pkGen = pkGen;
            this.ckGenSupplier = ckGenSupplier;
            this.deleteKindGen = deleteKindGen;
            this.columnDeleteGenerator = columnDeleteGenerator;
            this.timestampGen = timestampGen;
            this.generator = build(schemaSpec, pkGen, ckGenSupplier, deleteKindGen, columnDeleteGenerator, timestampGen);
        }

        public Builder withTimestamp(Gen<Long> timestamps)
        {
            return new Builder(schemaSpec, pkGen, ckGenSupplier, deleteKindGen, columnDeleteGenerator, timestamps.map(Optional::of));
        }

        public Builder withTimestamp(long ts)
        {
            return withTimestamp(arbitrary().constant(ts));
        }

        public Builder withCurrentTimestamp()
        {
            return withTimestamp(FBUtilities.timestampMicros());
        }

        // TODO: add static columns?
        public Builder deleteColumns()
        {
            // TODO: some sort of warning/validation to make sure we do not allow to generate this with ranges?
            return new Builder(schemaSpec, pkGen, ckGenSupplier, deleteKindGen,
                               Extensions.subsetGenerator(schemaSpec.regularColumns.stream()
                                                                                   .map(ColumnSpec::name)
                                                                                   .collect(Collectors.toList()))
                                         .map(Optional::of),
                               timestampGen);
        }

        private static QueryKind validate(Gen<Object[]> pkGen,
                                          Function<Object[], Gen<Object[]>> ckGenSupplier,
                                          QueryKind readType)
        {
            switch (readType)
            {
                case SINGLE_PARTITION:
                    if (pkGen == null)
                        throw new IllegalArgumentException("Need a partition key generator to generate partition deletes");
                    break;
                case SINGLE_ROW:
                case CLUSTERING_SLICE:
                case CLUSTERING_RANGE:
                    if (pkGen == null || ckGenSupplier == null)
                        throw new IllegalArgumentException("Need a partition and a clustering key key generator to generate row and slice reads");
            }

            return readType;
        }

        private static Gen<Delete> build(SchemaSpec schemaSpec,
                                        Gen<Object[]> pkGen,
                                        Function<Object[], Gen<Object[]>> ckGenSupplier,
                                        Gen<QueryKind> deleteKindGen,
                                        Gen<Optional<List<String>>> columnDeleteGenerator,
                                        Gen<Optional<Long>> timestampGen)
        {
            return prng -> {
                QueryKind kind = validate(pkGen, ckGenSupplier, deleteKindGen.generate(prng));
                List<Relation> relations = Relation.relationsGen(schemaSpec,
                                                                 pkGen,
                                                                 ckGenSupplier,
                                                                 kind).generate(prng);
                Optional<List<String>> columns = kind == QueryKind.SINGLE_ROW ? columnDeleteGenerator.generate(prng) : Optional.empty();
                Optional<Long> ts = timestampGen.generate(prng);
                return new Delete(schemaSpec,
                                  columns,
                                  relations,
                                  ts);
            };

        }

        public Delete generate(RandomnessSource prng)
        {
            return generator.generate(prng);
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
                bindings[i] = relations.get(i).value();
            }

            timestamp.ifPresent(delete::setDefaultTimestamp);
            return new CompiledStatement(delete.toString(), bindings);
        }

        @Override
        public String toString()
        {
            // A little wasteful, but necessary
            return compile().toString();
        }
    }

    public static String[] toArray(List<String> strings)
    {
        return strings.toArray(new String[strings.size()]);
    }
}