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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.SharedClusterTestBase;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.Offsets;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterAndBatchWitnessTest extends SharedClusterTestBase
{
    private static final int CLUSTER_SIZE = 3;
    private static final int REPLICATION_FACTOR = 3;
    private static final int TRANSIENT_REPLICAS = 1;
    private static final int FULL_REPLICAS = REPLICATION_FACTOR - TRANSIENT_REPLICAS;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        setupCluster(CLUSTER_SIZE, builder -> builder.withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                .with(Feature.GOSSIP)
                                                                .with(Feature.NATIVE_PROTOCOL)
                                                                .set("mutation_tracking_enabled", "true")
                                                                .set("transient_replication_enabled", "true")));
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                                    "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                    "AND replication_type='tracked'");
    }

    @Before
    @Override
    public void setUp()
    {
        super.setUp();
        SHARED_CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + tableName +
                                                 " (k int, ck int, c counter, PRIMARY KEY (k, ck))"));
    }

    @After
    @Override
    public void tearDown()
    {
        SHARED_CLUSTER.filters().reset();
        super.tearDown();
    }

    @Test
    public void testBasicCounterIncrementWithWitness()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 5 WHERE k = 1 AND ck = 0"), QUORUM);
        assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = 1 AND ck = 0"), QUORUM),
                   row(5L));

        verifyWitnessDoesNotHaveData(1, 0);

        verifyMutationTrackingForKey(1);
    }

    @Test
    public void testMultipleCounterUpdatesWithWitness()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 10 WHERE k = 1 AND ck = 0"), QUORUM);
        coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 5 WHERE k = 1 AND ck = 0"), QUORUM);
        coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c - 3 WHERE k = 1 AND ck = 0"), QUORUM);

        assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = 1 AND ck = 0"), QUORUM),
                   row(12L));

        verifyWitnessDoesNotHaveData(1, 0);
        verifyMutationTrackingForKey(1);
    }

    @Test
    public void testCounterBatchSamePartitionWithWitness()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        String batch = "BEGIN COUNTER BATCH " +
                       "UPDATE " + qualifiedTableName + " SET c = c + 1 WHERE k = 1 AND ck = 1; " +
                       "UPDATE " + qualifiedTableName + " SET c = c + 2 WHERE k = 1 AND ck = 2; " +
                       "UPDATE " + qualifiedTableName + " SET c = c + 3 WHERE k = 1 AND ck = 3; " +
                       "APPLY BATCH";

        coordinator.execute(batch, QUORUM);

        assertRows(coordinator.execute(withKeyspace("SELECT ck, c FROM %s." + tableName + " WHERE k = 1 AND ck IN (1, 2, 3)"), QUORUM),
                   row(1, 1L),
                   row(2, 2L),
                   row(3, 3L));

        verifyWitnessDoesNotHaveData(1, 1);
        verifyMutationTrackingForKey(1);
    }

    @Test
    public void testCounterBatchDifferentPartitionsWithWitness()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        String batch = "BEGIN COUNTER BATCH " +
                       "UPDATE " + qualifiedTableName + " SET c = c + 10 WHERE k = 1 AND ck = 0; " +
                       "UPDATE " + qualifiedTableName + " SET c = c + 20 WHERE k = 2 AND ck = 0; " +
                       "UPDATE " + qualifiedTableName + " SET c = c + 30 WHERE k = 3 AND ck = 0; " +
                       "APPLY BATCH";

        coordinator.execute(batch, QUORUM);

        assertRows(coordinator.execute(withKeyspace("SELECT k, c FROM %s." + tableName + " WHERE k IN (1, 2, 3) AND ck = 0"), QUORUM),
                   row(1, 10L),
                   row(2, 20L),
                   row(3, 30L));

        verifyMutationTrackingForKey(1);
    }

    @Test
    public void testMultipleCounterBatchesWithWitness()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        for (int i = 0; i < 5; i++)
        {
            String batch = "BEGIN COUNTER BATCH " +
                           "UPDATE " + qualifiedTableName + " SET c = c + 1 WHERE k = 1 AND ck = 0; " +
                           "UPDATE " + qualifiedTableName + " SET c = c + 1 WHERE k = 2 AND ck = 0; " +
                           "APPLY BATCH";
            coordinator.execute(batch, QUORUM);
        }

        assertRows(coordinator.execute(withKeyspace("SELECT k, c FROM %s." + tableName + " WHERE k IN (1, 2) AND ck = 0"), QUORUM),
                   row(1, 5L),
                   row(2, 5L));

        verifyMutationTrackingForKey(1);
    }

    @Test
    public void testCounterAtConsistencyOne()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 5 WHERE k = 1 AND ck = 0"), ONE);
        // Read at QUORUM to ensure we get consistent value
        assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = 1 AND ck = 0"), QUORUM),
                   row(5L));

        verifyMutationTrackingForKey(1, ONE);
    }

    @Test
    public void testCounterAtConsistencyAll()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 7 WHERE k = 1 AND ck = 0"), ALL);
        assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = 1 AND ck = 0"), ALL),
                   row(7L));

        verifyMutationTrackingForKey(1, ALL);
    }

    @Test
    public void testCounterForwardingFromNonReplicaCoordinator()
    {
        // Test counter writes from all nodes
        // Some will be local (coordinator is replica), some will be forwarded (coordinator not replica)
        for (int coordinatorNode = 1; coordinatorNode <= CLUSTER_SIZE; coordinatorNode++)
        {
            ICoordinator coordinator = SHARED_CLUSTER.coordinator(coordinatorNode);
            int pk = coordinatorNode * 100;

            coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 5 WHERE k = ? AND ck = 0"), QUORUM, pk);
            assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = ? AND ck = 0"), QUORUM, pk),
                       row(5L));

            coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 3 WHERE k = ? AND ck = 0"), QUORUM, pk);
            assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = ? AND ck = 0"), QUORUM, pk),
                       row(8L));
        }

        for (int node = 1; node <= CLUSTER_SIZE; node++)
        {
            ICoordinator coordinator = SHARED_CLUSTER.coordinator(node);
            for (int pk = 100; pk <= 300; pk += 100)
            {
                assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = ? AND ck = 0"), QUORUM, pk),
                           row(8L));
            }
        }
    }

    @Test
    public void testCounterReadAtSerialConsistency()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + 42 WHERE k = 1 AND ck = 0"), QUORUM);

        assertRows(coordinator.execute(withKeyspace("SELECT c FROM %s." + tableName + " WHERE k = 1 AND ck = 0"), ConsistencyLevel.SERIAL),
                   row(42L));

        verifyMutationTrackingForKey(1);
    }

    @Test
    public void testCounterRangeQuery()
    {
        ICoordinator coordinator = SHARED_CLUSTER.coordinator(1);

        for (int i = 1; i <= 5; i++)
        {
            coordinator.execute(withKeyspace("UPDATE %s." + tableName + " SET c = c + ? WHERE k = 1 AND ck = ?"), QUORUM, (long) i, i);
        }

        Object[][] result = coordinator.execute(withKeyspace("SELECT ck, c FROM %s." + tableName + " WHERE k = 1 AND ck >= 2 AND ck <= 4"), QUORUM);
        assertEquals(3, result.length);
        assertRows(result,
                   row(2, 2L),
                   row(3, 3L),
                   row(4, 4L));

        verifyMutationTrackingForKey(1);
    }

    private void verifyWitnessDoesNotHaveData(int k, int ck)
    {
        int nodesWithData = 0;
        for (int i = 1; i <= CLUSTER_SIZE; i++)
        {
            Object[][] result = SHARED_CLUSTER.get(i).executeInternal(
                "SELECT c FROM " + KEYSPACE + "." + tableName + " WHERE k = ? AND ck = ?", k, ck);
            if (result != null && result.length > 0 && result[0][0] != null)
            {
                nodesWithData++;
            }
        }

        assertEquals("Only full replicas should have counter data", FULL_REPLICAS, nodesWithData);
    }

    private void verifyMutationTrackingForKey(int key)
    {
        verifyMutationTrackingForKey(key, QUORUM);
    }

    private void verifyMutationTrackingForKey(int key, ConsistencyLevel cl)
    {
        int nodesWithMutationTracking = 0;
        for (int node = 1; node <= CLUSTER_SIZE; node++)
        {
            MutationSummary summary = MutationTrackingUtils.summaryForKey(SHARED_CLUSTER.get(node), KEYSPACE, tableName, key);
            if (summary.size() > 0)
            {
                CoordinatorLogId logId = getOnlyLogId(summary);
                Offsets offsets = summaryIdSpace(summary.get(logId));
                if (offsets.offsetCount() >= 1)
                    nodesWithMutationTracking++;
            }
        }

        int expectedMinimum;
        switch (cl)
        {
            case ONE:
                expectedMinimum = 1;
                break;
            case ALL:
                expectedMinimum = REPLICATION_FACTOR;
                break;
            case QUORUM:
                expectedMinimum = (REPLICATION_FACTOR / 2) + 1;
                break;
            default:
                throw new IllegalArgumentException("Add support for " + cl);
        }

        assertTrue("Expected at least " + expectedMinimum + " nodes with mutation tracking for " + cl +
                   ", got " + nodesWithMutationTracking,
                   nodesWithMutationTracking >= expectedMinimum);
    }
}
