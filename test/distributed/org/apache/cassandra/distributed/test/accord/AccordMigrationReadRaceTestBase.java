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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Ranges;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageSink;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationRepairResult;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigration;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static java.lang.String.format;
import static org.apache.cassandra.Util.spinUntilSuccess;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getNextEpoch;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseAfterEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;

/*
 * Test that non-transactional write operations such as regular mutations, batch log, and hints
 * all detect when a migration is in progress, and then retry on the correct system.
 */
public abstract class AccordMigrationReadRaceTestBase extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMigrationReadRaceTestBase.class);

    private static final String TABLE_FMT = "CREATE TABLE %s (id int, c int, v int, PRIMARY KEY ((id), c));";

    public static final int PKEY_ACCORD = 3;
    public static final int PKEY_NORMAL = 0;

    private static IPartitioner partitioner;

    private static Token minToken;

    private static Token maxToken;

    private static Token midToken;

    private static Token upperMidToken;

    private static Token lowerMidToken;

    private static ICoordinator coordinator;

    private final static TestMessageSink messageSink = new TestMessageSink();
    private static class TestMessageSink implements IMessageSink
    {
        private final Queue<Pair<InetSocketAddress,IMessage>> messages = new ConcurrentLinkedQueue<>();
        private final Set<InetSocketAddress> blackholed = new ConcurrentHashSet<>();

        public void reset()
        {
            messages.clear();
            blackholed.clear();
        }

        @Override
        public void accept(InetSocketAddress to, IMessage message) {
            messages.offer(Pair.create(to,message));
            IInstance i = SHARED_CLUSTER.get(to);
            if (blackholed.contains(to) || blackholed.contains(message.from()))
                return;
            if (i != null)
                i.receiveMessage(message);
        }
    }

    enum Scenario
    {
        // Apply the mutation from the coordinator directly without going through hinting
        MUTATION(false, false, false, false, false),
        // Hint from the initial mutation coordination
        HINT(true, false, true, false, true),
        // Apply the mutation from the batchlog directly
        BATCHLOG_SUCCESSFUL_ROUTING(false, true, true, true, false),
        // Have the batchlog use hints to apply the mutation after failing to route, migrating back from Accord this is a timeout because you can't get Accord to fail at routing
        // it either executes correctly in the old epoch or times out waiting for the new one to arrive
        BATCHLOG_FAILED_ROUTING_THEN_HINT(false, true, true, true, true),
        // Have the batchlog use hints to apply the mutation after a timeout
        BATCHLOG_FAILED_TIMEOUT_THEN_HINT(false, true, true, true, true),
        ;

        final boolean initiallyEnableHints;
        final boolean initiallyEnableBatchlogReplay;
        final boolean initiallyBlockTestKeyspaceMutations;
        final boolean passesThroughBatchlog;
        final boolean deliversViaHint;

        Scenario(boolean initiallyEnableHints, boolean initiallyEnableBatchlogReplay, boolean initiallyBlockTestKeyspaceMutations, boolean passesThroughBatchlog, boolean deliversViaHint)
        {
            this.initiallyEnableHints = initiallyEnableHints;
            this.initiallyEnableBatchlogReplay = initiallyEnableBatchlogReplay;
            this.initiallyBlockTestKeyspaceMutations = initiallyBlockTestKeyspaceMutations;
            this.passesThroughBatchlog = passesThroughBatchlog;
            this.deliversViaHint = deliversViaHint;
        }
    }

    private final boolean migrateAwayFromAccord;

    protected AccordMigrationReadRaceTestBase()
    {
        this.migrateAwayFromAccord = migratingAwayFromAccord();
    }

    protected abstract boolean migratingAwayFromAccord();

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        ServerTestUtils.daemonInitialization();
        // Otherwise repair complains if you don't specify a keyspace
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);
        AccordTestBase.setupCluster(builder -> builder.appendConfig(config -> config.set("paxos_variant", PaxosVariant.v2.name())
                                                                                    .set("write_request_timeout", "2s")
                                                                                    .set("accord.range_migration", "explicit")), 3);
        partitioner = FBUtilities.newPartitioner(SHARED_CLUSTER.get(1).callsOnInstance(() -> DatabaseDescriptor.getPartitioner().getClass().getSimpleName()).call());
        StorageService.instance.setPartitionerUnsafe(partitioner);
        ServerTestUtils.prepareServerNoRegister();
        minToken = partitioner.getMinimumToken();
        maxToken = partitioner.getMaximumTokenForSplitting();
        midToken = partitioner.midpoint(minToken, maxToken);
        upperMidToken = partitioner.midpoint(midToken, maxToken);
        lowerMidToken = partitioner.midpoint(minToken, midToken);
        coordinator = SHARED_CLUSTER.coordinator(1);
        SHARED_CLUSTER.setMessageSink(messageSink);
    }

    @AfterClass
    public static void tearDownClass()
    {
        StorageService.instance.resetPartitionerUnsafe();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
        messageSink.reset();
        SHARED_CLUSTER.forEach(ClusterUtils::clearAndUnpause);
        super.tearDown();
    }

    private NavigableMap<Integer, NavigableMap<Integer, Integer>> modelByPK = new TreeMap<>();
    private NavigableMap<Token, NavigableMap<Integer, Integer>> modelByToken = new TreeMap<>();

    @Before
    public void setUp() throws Throwable
    {
        Stopwatch sw = Stopwatch.createStarted();
        Random r = new Random(0);
        List<java.util.concurrent.Future<SimpleQueryResult>> inserts = new ArrayList<>();
        boolean buildModel = modelByPK.isEmpty();
        for (int i = 0; i < 10000; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                int pk = r.nextInt();
                int clustering = r.nextInt();
                inserts.add(coordinator.asyncExecuteWithResult(insertCQL(qualifiedAccordTableName, pk, clustering, 42), ALL));
                if (buildModel)
                {
                    NavigableMap<Integer, Integer> partition = modelByPK.computeIfAbsent(pk, newPK -> new TreeMap<>());
                    partition.put(clustering, 42);
                    modelByToken.put(Util.token(pk), partition);
                }
            }

            if (i % 500 == 0)
            {
                for (java.util.concurrent.Future<SimpleQueryResult> insert : inserts)
                    insert.get();
                inserts.clear();
            }
        }
        logger.info("Setup rows took %dms", sw.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testKeyRouting()
    {

    }

    private ListenableFuture<Void> alterTableTransactionalModeAsync(TransactionalMode mode)
    {
        ListenableFutureTask<Void> task = ListenableFutureTask.create(() -> {
            coordinator.execute(format("ALTER TABLE %s WITH %s", qualifiedAccordTableName, mode.asCqlParam()), ALL);
        }, null);
        Thread asyncThread = new Thread(task, "Alter table transaction mode " + mode);
        asyncThread.setDaemon(true);
        asyncThread.start();
        return task;
    }

    /*
     * Set up 3 to be behind and unaware of the migration while 1 and 2 are aware
     */
    private IInvokableInstance setUpOutOfSyncNode(Cluster cluster, Scenario scenario) throws Throwable
    {
        IInvokableInstance i1 = cluster.get(1);
        IInvokableInstance i2 = cluster.get(2);
        IInvokableInstance i3 = cluster.get(3);
        alterTableTransactionalMode(TransactionalMode.full);
        Epoch nextEpoch = getNextEpoch(i1);
        // Node 3 will coordinate the query and not be aware that the migration has begun
        Callable<?> pausedBeforeEnacting = pauseBeforeEnacting(i3, nextEpoch);
        // In batch log delivery cases i2 will be the coordinator and we need to be sure that it has enacted the latest epoch
        Callable<?> i2PausedAfterEnacting = pauseAfterEnacting(i2, nextEpoch);

        ListenableFuture<?> result = nodetoolAsync(coordinator, "consensus_admin", "begin-migration", "-st", midToken.toString(), "-et", maxToken.toString(), "-tp", "accord", KEYSPACE, accordTableName);

        if (migrateAwayFromAccord)
        {
            pausedBeforeEnacting.call();
            i2PausedAfterEnacting.call();
            unpauseEnactment(i2);
            unpauseEnactment(i3);
            result.get();
            long migratingEpoch = nextEpoch.getEpoch();
            Util.spinUntilTrue(() -> cluster.stream().allMatch(instance -> instance.callOnInstance(() -> ClusterMetadata.current().epoch.equals(Epoch.create(migratingEpoch)))), 10);
            nextEpoch = getNextEpoch(i1);
            pausedBeforeEnacting = pauseBeforeEnacting(i3, nextEpoch);
            i2PausedAfterEnacting = pauseAfterEnacting(i2, nextEpoch);
            // In the reverse direction doing the alter automatically reverses the migration without a need to call begin migration on any ranges
            result = alterTableTransactionalModeAsync(TransactionalMode.off);
        }

        // Wait for everyone to get to where they are supposed to be
        try
        {
            pausedBeforeEnacting.call();
        }
        catch (Throwable t)
        {
            if (result.isDone())
            {
                try
                {
                    result.get();
                }
                catch (ExecutionException e)
                {
                    t.addSuppressed(e);
                    throw t;
                }
            }
            throw t;
        }
        i2PausedAfterEnacting.call();
        // Unpause on 1 and 2 where we want them aware of the migration
        unpauseEnactment(i1);
        unpauseEnactment(i2);
        // nodetool should be able to complete now
        result.get();

        // Need to complete the migration for its eventual execution in the next epoch to be discovered to be misrouted
        // now that we continue to write through Accord during migration away from Accord
        // Faking the completed repair is the only way to get it in a state where two coordinators know about the new
        // epoch and one doesn't
        if (migrateAwayFromAccord && scenario.deliversViaHint && scenario.passesThroughBatchlog)
        {
            String keyspace = KEYSPACE;
            String table = accordTableName;
            long midTokenLong = midToken.getLongValue();
            long maxTokenLong = maxToken.getLongValue();
            SHARED_CLUSTER.get(1).runOnInstance(() ->
                {
                    Epoch startEpoch = ClusterMetadata.current().epoch;
                    Epoch epochAfterRepair = startEpoch.nextEpoch();
                    TableId tableId = Schema.instance.getTableMetadata(keyspace, table).id;
                    List<Range<Token>> ranges = ImmutableList.of(new Range<>(new LongToken(midTokenLong), new LongToken(maxTokenLong)));
                    RepairJobDesc desc = new RepairJobDesc(null, null, keyspace, table, ranges);
                    TokenRange range = new TokenRange(new TokenKey(tableId, new LongToken(midTokenLong)), new TokenKey(tableId, new LongToken(maxTokenLong)));
                    Ranges accordRanges = Ranges.of(range);
                    ConsensusMigrationRepairResult repairResult = ConsensusMigrationRepairResult.fromRepair(startEpoch, accordRanges, true, true, true, false);
                    ConsensusTableMigration.completedRepairJobHandler.onSuccess(new RepairResult(desc, null, repairResult));
                    spinUntilSuccess(() -> ClusterMetadata.current().epoch.equals(epochAfterRepair));
                });
        }

        return i3;
    }

    private static String insertCQL(String qualifiedTableName, int pkey, int clustering, int value)
    {
        return format("INSERT INTO %s ( id, c, v ) VALUES ( %d, %d, %d )", qualifiedTableName, pkey, clustering, value);
    }

    // Prevents the creation of transactions in an older epoch because later writes need to order after earlier
//    private void writeAccordRowViaAccord()
//    {
//        logger.info("Initiating Accord row write");
//        SHARED_CLUSTER.coordinator(1).execute(insertCQL(qualifiedAccordTableName, PKEY_ACCORD, 99), ConsistencyLevel.QUORUM);
//        logger.info("Finished Accord row write");
//    }
}
