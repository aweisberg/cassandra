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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

import static org.apache.cassandra.quicktheories.generators.Extensions.subsetGenerator;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;

public class ReadsDSL
{
    public enum ReadType
    {
        SINGLE_PARTITION,
        // TODO (alexp): implement these
//        MULTI_PARTITION,
//        PARTITION_RANGE_OPEN,
//        PARTITION_RANGE_CLOSED,
//        MULTI_CLUSTERING_SLICE,
//        MULTI_CLUSTERING_RANGE,
        SINGLE_ROW,
        CLUSTERING_SLICE,
        CLUSTERING_RANGE
    }

    public ReadsBuilder partitionRead(SchemaSpec schemaSpec)
    {
        return new ReadsBuilder(schemaSpec,
                                schemaSpec.partitionKeyGenerator,
                                null,
                                ReadType.SINGLE_PARTITION);
    }

    public ReadsBuilder partitionRead(SchemaSpec schemaSpec, Object[] pk)
    {
        return new ReadsBuilder(schemaSpec,
                                Generate.constant(pk),
                                null,
                                ReadType.SINGLE_PARTITION);
    }

    public ReadsBuilder rowRead(SchemaSpec schemaSpec)
    {
        assert schemaSpec.clusteringKeys.size() > 0 : "Can't read a row from the table that has no clustering columns";

        return new ReadsBuilder(schemaSpec,
                                schemaSpec.partitionKeyGenerator,
                                (pk) -> schemaSpec.clusteringKeyGenerator,
                                ReadType.SINGLE_ROW);
    }

    public ReadsBuilder rowRead(SchemaSpec schemaSpec, Gen<Object[]> pkGen, Function<Object[], Gen<Object[]>> ckGen)
    {
        return new ReadsBuilder(schemaSpec,
                                pkGen,
                                ckGen,
                                ReadType.SINGLE_ROW);
    }

    /**
     * Generates a row slice, e.g. (ck, ∞), (∞, ck), [ck, ∞), (∞, ck]
     */
    public ReadsBuilder rowSlice(SchemaSpec schemaSpec)
    {
        assert schemaSpec.clusteringKeys.size() > 0 : "Can't read a row slice from the table that has no clustering columns";

        return new ReadsBuilder(schemaSpec,
                                schemaSpec.partitionKeyGenerator,
                                (pk) -> schemaSpec.clusteringKeyGenerator,
                                ReadType.CLUSTERING_SLICE);
    }

    /**
     * Generates a row slice, e.g., inclusive or exclusive range between two clusterings
     */
    public ReadsBuilder rowRange(SchemaSpec schemaSpec)
    {
        return new ReadsBuilder(schemaSpec,
                                schemaSpec.partitionKeyGenerator,
                                (pk) -> schemaSpec.clusteringKeyGenerator,
                                ReadType.CLUSTERING_RANGE);
    }

    private static Gen<List<String>> columnSubsetGen(SchemaSpec schemaSpec)
    {
        if (schemaSpec.allColumns.size() == 0)
            throw new IllegalArgumentException("Can't generate a subset of an empty set");

        return subsetGenerator(schemaSpec.allColumns, 1, schemaSpec.allColumns.size() == 1 ? 1 : schemaSpec.allColumns.size() - 1)
               // We can only restrict clustering columns when selecting non-static columns
               // using `map` instead of `assuming` to avoid running out of variants
               .map(columnDefinitions -> {
                   if (columnDefinitions.stream().allMatch((cd) -> cd.kind == ColumnMetadata.Kind.STATIC ||
                                                                   cd.kind == ColumnMetadata.Kind.PARTITION_KEY))
                       return schemaSpec.allColumns;
                   return columnDefinitions;
               })
               .map(cds -> cds.stream().map(ColumnSpec::name).collect(Collectors.toList()));
    }

    private static Gen<List<Pair<String, Boolean>>> clusteringOrderGen(List<ColumnSpec<?>> columnSpecs)
    {
        return booleans().all().map(isReversed -> {
            List<Pair<String, Boolean>> orderings = new ArrayList<>();
            // TODO (alexp): try same with _not all_ columns in ordering
            for (ColumnSpec<?> columnSpec : columnSpecs)
            {
                orderings.add(Pair.create(columnSpec.name,
                                          isReversed ? columnSpec.isReversed() : !columnSpec.isReversed()));
            }

            return orderings;
        });
    }

    private static Gen<Sign> signGen = Generate.enumValues(Sign.class);
    private static Gen<Sign> sliceSigns = Generate.pick(Arrays.asList(Sign.GT, Sign.GTE, Sign.LT, Sign.LTE));

    public static class ReadsBuilder
    {
        private final SchemaSpec schemaSpec;
        private final Gen<Object[]> pkGen;
        private final Function<Object[], Gen<Object[]>> ckGenSupplier;
        private final ReadType readType;

        private boolean wildcard = true;
        private boolean addLimit = false;
        private boolean addOrder = false;

        ReadsBuilder(SchemaSpec schemaSpec,
                     Gen<Object[]> pkGen,
                     Function<Object[], Gen<Object[]>> ckGenSupplier,
                     ReadType readType)
        {
            this.schemaSpec = schemaSpec;
            this.pkGen = pkGen;
            this.ckGenSupplier = ckGenSupplier;
            this.readType = readType;
        }

        public ReadsBuilder withColumnSelection()
        {
            assert readType != ReadType.SINGLE_ROW ||
                   schemaSpec.regularColumns.size() > 0 : "Can't use partition-level only selection when querying a row";

            this.wildcard = false;
            return this;
        }

        public ReadsBuilder withLimit()
        {
            this.addLimit = true;
            return this;
        }

        public ReadsBuilder withOrder()
        {
            this.addOrder = true;
            return this;
        }

        private void validate()
        {
            switch (readType)
            {
                case SINGLE_PARTITION:
                    assert pkGen != null;
                    return;
                case SINGLE_ROW:
                case CLUSTERING_SLICE:
                case CLUSTERING_RANGE:
                    assert pkGen != null;
                    assert ckGenSupplier != null;
            }
        }

        private Gen<List<Relation>> relationsGen()
        {
            return (prng) -> {
                List<Relation> relations = new ArrayList<>();
                switch (readType)
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

        private void addRelation(Object[] pk, List<ColumnSpec<?>> columnSpecs, List<Relation> relations)
        {
            for (int i = 0; i < pk.length; i++)
            {
                ColumnSpec<?> spec = columnSpecs.get(i);
                relations.add(new Relation(spec.name, Sign.EQ, pk[i]));
            }
        }

        // TODO (alexp): make it possible to provide columns
        public Gen<Select> build()
        {
            Gen<Optional<List<String>>> selectionGen = wildcard
                                                       ? Generate.constant(Optional.empty())
                                                       : columnSubsetGen(schemaSpec).map(Optional::of);

            Gen<Optional<Integer>> limitGen = addLimit
                                              ? integers().between(1, 100).map(Optional::of) // TODO: to make limit configurable
                                              : Generate.constant(Optional.empty());

            Gen<List<Pair<String, Boolean>>> orderingGen = addOrder
                                                           ? clusteringOrderGen(schemaSpec.clusteringKeys)
                                                           : Generate.constant(Collections.EMPTY_LIST);

            return selectionGen.zip(relationsGen(),
                                    limitGen,
                                    orderingGen,
                                    (selection, relations, limit, order) -> {
                                        return new Select(schemaSpec,
                                                          selection,
                                                          relations,
                                                          limit,
                                                          order);
                                    });
        }
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

        public String toCQL()
        {
            return column + " " + sign + " ?";
        }
    }

    public static Relation relation(String column, Sign sign, Object value)
    {
        return new Relation(column, sign, value);
    }

    public static class Select
    {
        public final SchemaSpec schemaSpec;
        public final Optional<List<String>> selectedColumns;
        public final List<Relation> relations;
        public final Optional<Integer> limit;
        public final List<Pair<String, Boolean>> ordering;
        // TODO: filtering
        public Select(SchemaSpec schemaSpec,
                      // TODO (alexp): do we need optional here?
                      Optional<List<String>> selectedColumns,
                      List<Relation> relations,
                      Optional<Integer> limit,
                      List<Pair<String, Boolean>> ordering)
        {
            this.schemaSpec = schemaSpec;
            this.selectedColumns = selectedColumns;
            this.relations = relations;
            this.limit = limit;
            this.ordering = ordering;
        }


        public Pair<String, Object[]> compile()
        {
            Object[] bindings = new Object[relations.size()];
            int bindingCount = 0;
            StringBuilder builder = new StringBuilder();
            if (selectedColumns.isPresent())
            {
                builder.append("SELECT ");
                SchemaSpec.SeparatorAppender ca = new SchemaSpec.SeparatorAppender();
                for (String s : selectedColumns.get())
                    ca.accept(builder, s);
            }
            else
            {
                builder.append("SELECT *");
            }
            builder.append(" FROM ")
                   .append(schemaSpec.ksName)
                   .append('.')
                   .append(schemaSpec.tableName)
                   .append(' ');

            if (!relations.isEmpty())
            {
                builder.append(" WHERE ");
                SchemaSpec.SeparatorAppender ca = new SchemaSpec.SeparatorAppender(" AND ");
                for (Relation relation : relations)
                {
                    ca.accept(builder, relation.toCQL());
                    bindings[bindingCount++] = relation.value;
                }
            }

            if (!ordering.isEmpty())
            {
                builder.append(" ORDER BY");
                SchemaSpec.SeparatorAppender ca = new SchemaSpec.SeparatorAppender();
                for (Pair<String, Boolean> tuple : ordering)
                {
                    builder.append(" ");
                    ca.accept(builder, tuple.left + " " + (tuple.right ? "ASC" : "DESC"));
                }
            }

            limit.ifPresent(integer -> builder.append(" LIMIT ").append(integer));

            // TODO (alexp): order
            assert bindingCount == bindings.length : bindingCount + " != " + bindings.length;
            return Pair.create(builder.toString(), bindings);
        }

        public String toString()
        {
            return "Select{" +
                   "selectedColumns=" + selectedColumns +
                   ", relations=" + relations +
                   ", limit=" + limit +
                   ", ordering=" + ordering +
                   '}';
        }
    }
}
