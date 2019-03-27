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

package org.apache.cassandra.quicktheories.tests;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.quicktheories.generators.ColumnSpec;
import org.apache.cassandra.quicktheories.generators.CompiledStatement;
import org.apache.cassandra.quicktheories.generators.FullKey;
import org.apache.cassandra.quicktheories.generators.SchemaSpec;
import org.apache.cassandra.quicktheories.generators.Sign;
import org.apache.cassandra.quicktheories.generators.WritesDSL;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.stateful.StatefulTheory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static org.apache.cassandra.distributed.test.DistributedTestBase.assertRows;
import static org.apache.cassandra.quicktheories.generators.Extensions.combine;
import static org.apache.cassandra.quicktheories.generators.Extensions.subsetGenerator;

public abstract class StatefulModel extends StatefulTheory.StepBased
{
    private final AbstractCluster testCluster;
    private final AbstractCluster modelCluster;
    protected final ModelState modelState;
    protected final Gen<Integer> nodeSelector;
    protected SchemaSpec schemaSpec;

    public StatefulModel(ModelState modelState,
                         AbstractCluster testCluster,
                         AbstractCluster modelCluster)
    {
        this.modelState = modelState;
        this.testCluster = testCluster;
        this.modelCluster = modelCluster;
        this.nodeSelector = SourceDSL.integers().between(1, testCluster.size());
    }

    public void run(Select query, int node)
    {
        Iterator<Object[]> modelRows;
        Iterator<Object[]> sutRows;
        try
        {
            modelRows = Iterators.forArray(testCluster.get(1).executeInternal(query.toString()));
            sutRows = testCluster.coordinator(node).executeWithPaging(query.toString(),
                                                                      ConsistencyLevel.QUORUM,
                                                                      2);
            assertRows(modelRows, sutRows);
        }
        catch (Exception e)
        {
            System.out.println("query = " + query);
            reportAndExit(e, node);
        }
    }

    private void reportAndExit(Throwable t, int node)
    {
        System.out.println("Node: " + node);
        System.out.println(schemaSpec.toCQL());
        System.err.println(t.getMessage());
        t.printStackTrace();
        System.exit(1);
    }

    protected void insertRow(WritesDSL.DataRow row, int node)
    {
        try
        {
            modelState.addFullKey(row.key());
            CompiledStatement compiledStatement = row.compile();

            modelCluster.get(1).executeInternal(compiledStatement.cql(), compiledStatement.bindings());
            testCluster.coordinator(node).execute(compiledStatement.cql(),
                                                  ConsistencyLevel.QUORUM,
                                                  compiledStatement.bindings());
        }
        catch (Throwable t)
        {
            // TODO (alexp): make stateful print stack trace instead of just saying that stuff has failed
            System.out.println("t = " + t);
            t.printStackTrace();
        }
    }

    protected void insertRows(List<WritesDSL.Insert> rows, int node)
    {
        for (WritesDSL.DataRow row : rows)
        {
            insertRow(row, node);
        }
    }

    public void initSchema(SchemaSpec schemaSpec)
    {
        assert this.schemaSpec == null : "Schema was already initialized";
        this.schemaSpec = schemaSpec;
        String ddl = schemaSpec.toCQL();
        System.out.println("CREATING SCHEMA: \n" + ddl);
        modelCluster.schemaChange(ddl);
        testCluster.schemaChange(ddl);
    }


}
