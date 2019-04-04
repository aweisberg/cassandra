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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.quicktheories.generators.Extensions.subsetGenerator;

// TODO: rename consistently with the other classes
public class SchemaSpec
{
    public final String ksName;
    public final String tableName;
    public final List<ColumnSpec<?>> partitionKeys;
    public final List<ColumnSpec<?>> clusteringKeys;
    public final List<ColumnSpec<?>> staticColumns;
    public final List<ColumnSpec<?>> regularColumns;
    public final Gen<Object[]> partitionKeyGenerator;
    public final Gen<Object[]> clusteringKeyGenerator;
    public final Gen<List<Pair<ColumnSpec<?>, Object>>> rowDataGenerator;
    public final List<ColumnSpec<?>> allColumns;

    // TODO: improve SchemaSpec builder that would allow configuring amount of columns etc
    SchemaSpec(String ksName,
               String tableName,
               List<ColumnSpec<?>> partitionKeys,
               List<ColumnSpec<?>> clusteringKeys,
               List<ColumnSpec<?>> staticColumns,
               List<ColumnSpec<?>> regularColumns)
    {
        this.ksName = ksName;
        this.tableName = tableName;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.staticColumns = staticColumns;
        this.regularColumns = regularColumns;

        // TODO: move this to static method; schema should not be able to generate anything
        this.partitionKeyGenerator = keyGenerator(partitionKeys);
        this.clusteringKeyGenerator = keyGenerator(clusteringKeys);
        this.rowDataGenerator = dataGenerator(Streams.concat(regularColumns.stream(),
                                                             staticColumns.stream())
                                                     .collect(toList()));

        this.allColumns = Streams.concat(partitionKeys.stream(),
                                         clusteringKeys.stream(),
                                         staticColumns.stream(),
                                         regularColumns.stream())
                                 .collect(toList());
    }

    static Gen<Object[]> keyGenerator(List<ColumnSpec<?>> columns)
    {
        return in -> {
            Object[] keys = new Object[columns.size()];
            for (int i = 0; i < keys.length; i++)
            {
                keys[i] = columns.get(i).generator.generate(in);
            }
            return keys;
        };
    }


    static Gen<List<Pair<ColumnSpec<?>, Object>>> dataGenerator(List<ColumnSpec<?>> columns)
    {
        return in -> {
            List<Pair<ColumnSpec<?>, Object>> pairs = new ArrayList<>(columns.size());
            for (ColumnSpec<?> cd : columns)
            {
                pairs.add(Pair.create(cd, cd.generator.generate(in)));
            }
            return pairs;
        };
    }

    public CompiledStatement compile()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        sb.append(ksName)
          .append(".")
          .append(tableName)
          .append(" (");

        SeparatorAppender commaAppender = new SeparatorAppender();
        for (ColumnSpec cd : partitionKeys)
        {
            commaAppender.accept(sb);
            sb.append(cd.toCQL());
            if (partitionKeys.size() == 1 && clusteringKeys.size() == 0)
                sb.append(" PRIMARY KEY");
        }

        Streams.concat(clusteringKeys.stream(),
                       staticColumns.stream(),
                       regularColumns.stream())
               .forEach((cd) -> {
                   commaAppender.accept(sb);
                   sb.append(cd.toCQL());
               });

        if (clusteringKeys.size() > 0 || partitionKeys.size() > 1)
        {
            sb.append(", ").append(getPrimaryKeyCql());
        }

        sb.append(')')
          .append(getClusteringOrderCql())
          .append(';');

        return new CompiledStatement(sb.toString(), new Object[]{});
    }

    private String getClusteringOrderCql()
    {
        StringBuilder sb = new StringBuilder();
        if (clusteringKeys.size() > 0)
        {
            sb.append(" WITH CLUSTERING ORDER BY (");

            SeparatorAppender commaAppender = new SeparatorAppender();
            for (ColumnSpec<?> cd : clusteringKeys)
            {
                commaAppender.accept(sb);
                sb.append(cd.name).append(' ').append(cd.type.isReversed() ? "DESC" : "ASC");
            }
            sb.append(")");
        }
        return sb.toString();
    }

    private String getPrimaryKeyCql()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("PRIMARY KEY (");
        if (partitionKeys.size() > 1)
        {
            sb.append('(');
            SeparatorAppender commaAppender = new SeparatorAppender();
            for (ColumnSpec<?> cd : partitionKeys)
            {
                commaAppender.accept(sb);
                sb.append(cd.name);
            }
            sb.append(')');
        }
        else
        {
            sb.append(partitionKeys.get(0).name);
        }

        for (ColumnSpec<?> cd : clusteringKeys)
            sb.append(", ").append(cd.name);

        return sb.append(')').toString();
    }

    public String toString()
    {
        return "SchemaSpec{" +
               "tableName='" + tableName + '\'' +
               ", partitionKeys=" + toString(partitionKeys) +
               ", clusteringKeys=" + toString(clusteringKeys) +
               ", staticColumns=" + toString(staticColumns) +
               ", regularColumns=" + toString(regularColumns) +
               '}';
    }

    private static String toString(List<ColumnSpec<?>> specs)
    {
        return specs.stream().map(c -> String.format("%s(%s)",
                                                     c.name,
                                                     c.type.asCQL3Type().toString()))
                    .collect(Collectors.joining(", "));
    }

    public static class SeparatorAppender implements Consumer<StringBuilder>
    {
        boolean isFirst = true;
        private final String separator;

        public SeparatorAppender()
        {
            this(",");
        }
        public SeparatorAppender(String separator)
        {
            this.separator = separator;
        }
        public void accept(StringBuilder stringBuilder)
        {
            if (isFirst)
                isFirst = false;
            else
                stringBuilder.append(separator);
        }

        public void accept(StringBuilder stringBuilder, String s)
        {
            accept(stringBuilder);
            stringBuilder.append(s);
        }


        public void reset()
        {
            isFirst = true;
        }
    }
}
