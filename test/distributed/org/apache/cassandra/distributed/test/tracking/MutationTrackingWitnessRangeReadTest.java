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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

/**
 * A witness journals a mutation so that it can take part in reconciliation, but never applies it to the table, so it
 * has no data to answer a read with. A tracked read reads data from exactly one replica, which therefore has to be a
 * full replica of every token in the range the read covers.
 */
public class MutationTrackingWitnessRangeReadTest extends TestBaseImpl
{
    private static final int REPLICAS = 3;
    private static final int PARTITIONS = 100;
    private static final String KS = "witness_range_read";

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                               .set("transient_replication_enabled", "true"))
                         .start();

        cluster.schemaChange(cql("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'} AND replication_type='tracked'"));
        cluster.schemaChange(cql("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int) WITH read_repair = 'NONE'"));
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    /**
     * Each node is the witness of one of the three primary ranges, so a scan of the whole ring covers a range that no
     * one replica is full for. Whichever node coordinates it, and whichever replica that node reads data from, the
     * answer is still the whole table.
     */
    @Test
    public void testFullTableScanFromEveryCoordinator()
    {
        Map<Integer, Integer> expected = new TreeMap<>();
        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            cluster.coordinator(1).execute(cql("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ConsistencyLevel.ALL, pk, pk);
            expected.put(pk, pk);
        }

        assertWitnessingIsInEffect();

        for (int node = 1; node <= REPLICAS; node++)
        {
            Object[][] rows = cluster.coordinator(node).execute(cql("SELECT pk, v FROM %s.tbl"), ConsistencyLevel.ALL);
            Map<Integer, Integer> actual = new TreeMap<>();
            for (Object[] row : rows)
                Assert.assertNull("partition " + row[0] + " returned twice", actual.put((Integer) row[0], (Integer) row[1]));
            Assert.assertEquals("full table scan coordinated on node " + node, expected, actual);
        }
    }

    /**
     * The unstressed case check, made with executeInternal so that it cannot reconcile away what it is measuring: a
     * witness journals a mutation without applying it to the table, so a node that witnesses a third of the ring holds
     * less than the whole table locally. If any node held all of it then nothing is being witnessed and a scan reading
     * data from one replica would be correct however the range was split.
     */
    private static void assertWitnessingIsInEffect()
    {
        for (int node = 1; node <= REPLICAS; node++)
        {
            int local = cluster.get(node).executeInternal(cql("SELECT pk FROM %s.tbl")).length;
            Assert.assertTrue("Not stressed: node " + node + " holds all " + PARTITIONS + " partitions, so it witnesses none of them",
                              local < PARTITIONS);
            Assert.assertTrue("node " + node + " holds no data at all, so it is not a full replica of anything",
                              local > 0);
        }
    }

    private static String cql(String template)
    {
        return String.format(template, KS);
    }
}
