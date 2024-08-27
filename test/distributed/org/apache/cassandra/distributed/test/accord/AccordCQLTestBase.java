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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AccordCQLTestBase extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCQLTestBase.class);

    private final TransactionalMode transactionalMode;

    protected AccordCQLTestBase(TransactionalMode transactionalMode) {
        this.transactionalMode = transactionalMode;
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder, 2);
        SHARED_CLUSTER.schemaChange("CREATE TYPE " + KEYSPACE + ".person (height int, age int)");
    }

    @Test
    public void testRangeReadPageOne() throws Exception
    {
        testRangeRead(1);
    }

    @Test
    public void testRangeReadSmallPage() throws Exception
    {
        testRangeRead(2);
    }

    @Test
    public void testRangeReadExactPage() throws Exception
    {
        testRangeRead(100);
    }

    @Test
    public void testRangeReadLargePage() throws Exception
    {
        testRangeRead(200);
    }

    @Test
    public void testRangeReadClosePageLT() throws Exception
    {
        testRangeRead(99);
    }

    @Test
    public void testRangeReadClosePageGT() throws Exception
    {
        testRangeRead(101);
    }

    private void testRangeRead(int pageSize) throws Exception
    {
        test(cluster -> {
            System.out.println("testRangeRead(" + pageSize + ")");
            Random r = new Random(0);
            Map<Pair<Integer, Integer>, Object[]> insertedRows = new HashMap<>();
            for (int i = 0; i < 10; i++)
            {
                int k = r.nextInt();
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + "(k, c, v) VALUES (?, ?, ?);", ConsistencyLevel.ALL, k, j, i + j);
                    insertedRows.put(Pair.create(k, j), new Object[] {k, j, i + j});
                }
            }

            Iterator<Object[]> iterator = cluster.coordinator(1).executeWithPaging("SELECT * FROM " + qualifiedAccordTableName + " WHERE TOKEN(k) > " + Long.MIN_VALUE + " AND TOKEN(k) < " + Long.MAX_VALUE, ConsistencyLevel.ALL, pageSize);
            List<Object[]> resultRows = ImmutableList.copyOf(iterator);
            System.out.println("Found rows:");
            resultRows.forEach(row -> System.out.println(Arrays.toString(row)));
            Integer lastPartitionKey = null;
            int currentRowKey = 0;
            for (Object[] row : resultRows)
            {
                assertEquals(currentRowKey, row[1]);

                if (lastPartitionKey == null)
                    lastPartitionKey = (Integer)row[0];
                else
                    assertEquals(lastPartitionKey, row[0]);

                if (currentRowKey == 9)
                {
                    currentRowKey = 0;
                    lastPartitionKey = null;
                }
                else
                    currentRowKey++;

                Object[] expected = insertedRows.remove(Pair.create(row[0], row[1]));
                assertEquals(expected, row);
            }
            System.out.println("Didn't find expected rows:");
            insertedRows.entrySet().forEach(row -> System.out.println(row));
            assertTrue(insertedRows.isEmpty());
        });
    }
}
