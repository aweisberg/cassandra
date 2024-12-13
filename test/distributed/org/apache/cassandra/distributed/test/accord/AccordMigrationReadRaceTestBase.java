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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Ranges;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static java.lang.String.format;
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getNextEpoch;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseAfterEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;
import static org.apache.cassandra.distributed.test.accord.AccordMigrationReadRaceTestBase.Scenario.ANY;
import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.junit.Assert.assertEquals;

/*
 * Test that non-transactional read operations migrating to/from a mode where Accord ignores commit consistency levels
 * and does aysnc commit are routed correctly. Currently this is just TransactionalMode.full
 */
public class AccordMigrationReadRaceTestBase extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMigrationReadRaceTestBase.class);

    private static final String TABLE_FMT = "CREATE TABLE %s (id int, c int, v int, PRIMARY KEY ((id), c));";

    private static IPartitioner partitioner;

    private static Range<Token> migratingRange;

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
        ANY;
        Scenario()
        {
        }
    }

    private final boolean migrateAwayFromAccord;

    public AccordMigrationReadRaceTestBase()
    {
        this.migrateAwayFromAccord = migratingAwayFromAccord();
    }

    protected boolean migratingAwayFromAccord()
    {
        return false;
    }

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
                                                                                    .set("read_request_timeout", "2s")
                                                                                    .set("native_transport_timeout", "3600s")
                                                                                    .set("accord.range_migration", "explicit")), 3);
        partitioner = FBUtilities.newPartitioner(SHARED_CLUSTER.get(1).callsOnInstance(() -> DatabaseDescriptor.getPartitioner().getClass().getSimpleName()).call());
        StorageService.instance.setPartitionerUnsafe(partitioner);
        ServerTestUtils.prepareServerNoRegister();
        LongToken migrationStart = new LongToken(Long.valueOf(SHARED_CLUSTER.get(2).callOnInstance(() -> DatabaseDescriptor.getInitialTokens().iterator().next())));
        LongToken migrationEnd = new LongToken(Long.valueOf(SHARED_CLUSTER.get(3).callOnInstance(() -> DatabaseDescriptor.getInitialTokens().iterator().next())));
        migratingRange = new Range<>(migrationStart, migrationEnd);
        coordinator = SHARED_CLUSTER.coordinator(1);
        SHARED_CLUSTER.setMessageSink(messageSink);
        buildData();
    }

    private static final Integer[][][] data = new Integer[1000][][];
    private static int pkeyAccord;
    private static int pkeyAccordDataIndex;

    private static void buildData()
    {
        Random r = new Random(0);
        for (int i = 0; i < 1000; i++)
        {
            data[i] = new Integer[10][];
            int pk = r.nextInt();
            for (int j = 0; j < 10; j++)
            {
                int clustering = r.nextInt();
                data[i][j] = new Integer[] { pk, clustering, 42 };
                LongToken token = Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(pk));
                if (migratingRange.contains(token))
                {
                    pkeyAccord = pk;
                    pkeyAccordDataIndex = i;
                }
            }
            Arrays.sort(data[i], Comparator.comparing(row -> row[1]));
        }
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

    private void loadData() throws Throwable
    {
        logger.info("Starting data load");
        Stopwatch sw = Stopwatch.createStarted();
        List<java.util.concurrent.Future<SimpleQueryResult>> inserts = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
        {
            for (int j = 0; j < 10; j++)
                inserts.add(coordinator.asyncExecuteWithResult(insertCQL(qualifiedAccordTableName, (int)data[i][j][0], (int)data[i][j][1], (int)data[i][j][2]), ALL));

            if (i % 100 == 0)
            {
                for (java.util.concurrent.Future<SimpleQueryResult> insert : inserts)
                    insert.get();
                inserts.clear();
            }
        }
        logger.info("Data load took %dms", sw.elapsed(TimeUnit.MILLISECONDS));
    }

    /*
     * Test cases
     * single partition
     *    read success on target
     *    read success not on target
     *    Retry different system
     * Range
     *    """"
     */
    @Test
    public void testKeyRouting() throws Throwable
    {
       String readCQL = "SELECT * FROM " + qualifiedAccordTableName + " WHERE id = " + pkeyAccord;
       testSplitAndRetry(readCQL, result -> assertThat(result).isDeepEqualTo(data[pkeyAccordDataIndex]), ANY);
    }

    @Test
    public void testRangeRouting() throws Throwable
    {
        String cql = "SELECT * FROM " + qualifiedAccordTableName + " WHERE token > " + bytesToHex(LongToken.keyForToken(Murmur3Partitioner.MINIMUM));
        testSplitAndRetry(cql, result -> {}, ANY);
    }

    private void testSplitAndRetry(String readCQL, Consumer<SimpleQueryResult> validation, Scenario scenario) throws Throwable
    {
        test(createTables(TABLE_FMT, qualifiedAccordTableName),
             cluster -> {
                 loadData();
                 // Node 3 is always the out of sync node
                 IInvokableInstance outOfSyncInstance = setUpOutOfSyncNode(cluster, scenario);
                 ICoordinator coordinator = outOfSyncInstance.coordinator();
                 int startRetryCount = getReadRetryOnDifferentSystemCount(outOfSyncInstance);
                 // If testing routing at mutation coordination then Node 1 and 2 will both rejected the mutation because it is in a migrating range
                 int startRejectedCount = getReadsRejectedOnWrongSystemCount();
                 logger.info("Executing read " + readCQL);
                 Future<SimpleQueryResult> resultFuture = coordinator.asyncExecuteWithResult(readCQL, ALL);

                 spinAssertEquals(startRejectedCount + 2, 10, () -> getReadsRejectedOnWrongSystemCount() - startRejectedCount);

                 logger.info("Unpausing out of sync instance");
                 // Testing regular mutation coordination retry loop let coordinator get up to date and retry
                 unpauseEnactment(outOfSyncInstance);

                 try
                 {
                     SimpleQueryResult result = resultFuture.get();
                     logger.info(result.toString());
                     validation.accept(result);
                 }
                 catch (ExecutionException e)
                 {
//                     // This is expected when inverting the migration
//                     if (migrateAwayFromAccord && e.getCause() instanceof CoordinatorBehindException)
//                         throw e;
                     throw e;
                 }

                 int endRetryCount = getReadRetryOnDifferentSystemCount(outOfSyncInstance);
                 int endRejectedCount = getReadsRejectedOnWrongSystemCount();
                 assertEquals(1, endRetryCount - startRetryCount);
                 // Expect only two nodes to reject since they enacted the new epoch
                 assertEquals(2, endRejectedCount - startRejectedCount);
             });
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
     * Set up 3 to be behind and unaware of the migration having progressed to the point where reads need to
     * be on a different system while 1 and 2 are aware
     */
    private IInvokableInstance setUpOutOfSyncNode(Cluster cluster, Scenario scenario) throws Throwable
    {
        IInvokableInstance i1 = cluster.get(1);
        IInvokableInstance i2 = cluster.get(2);
        IInvokableInstance i3 = cluster.get(3);
        alterTableTransactionalMode(TransactionalMode.full);
        // Reads are allowed until Accord thinks it owns the range and can start doing async commit and ignoring consistency levels
        nodetool(coordinator, "consensus_admin", "begin-migration", "-st", migratingRange.left.toString(), "-et", migratingRange.right.toString(), "-tp", "accord", KEYSPACE, accordTableName);
        // First repair only does the data and allows Accord to read, but doesn't require reads to be done through Accord
        nodetool(i2, "repair", "-skip-paxos", "-skip-accord", "-st", migratingRange.left.toString(), "-et", migratingRange.right.toString(), KEYSPACE, accordTableName);

        Epoch nextEpoch = getNextEpoch(i1);
        // Node 3 will coordinate the query and not be aware that the migration has begun
        Callable<?> pausedBeforeEnacting = pauseBeforeEnacting(i3, nextEpoch);

        // Wawnt to make sure both instances are aware of the migration
        Callable<?> i1PausedAfterEnacting = pauseAfterEnacting(i1, nextEpoch);
        Callable<?> i2PausedAfterEnacting = pauseAfterEnacting(i2, nextEpoch);

        // Unfortunately can't run real repair because it can't complete with i3 not responding because it's stuck waiting
        // on TCM so fake the completion of the repair by invoking the completion handler directly
        String keyspace = KEYSPACE;
        String table = accordTableName;
        long migratingTokenStart = migratingRange.left.getLongValue();
        long migratingTokenEnd = migratingRange.right.getLongValue();
        Future<?> result = SHARED_CLUSTER.get(1).asyncRunsOnInstance(() ->
                                            {
                                                Epoch startEpoch = ClusterMetadata.current().epoch;
                                                TableId tableId = Schema.instance.getTableMetadata(keyspace, table).id;
                                                List<Range<Token>> ranges = ImmutableList.of(new Range<>(new LongToken(migratingTokenStart), new LongToken(migratingTokenEnd)));
                                                RepairJobDesc desc = new RepairJobDesc(null, null, keyspace, table, ranges);
                                                TokenRange range = new TokenRange(new TokenKey(tableId, new LongToken(migratingTokenStart)), new TokenKey(tableId, new LongToken(migratingTokenEnd)));
                                                Ranges accordRanges = Ranges.of(range);
                                                ConsensusMigrationRepairResult repairResult = ConsensusMigrationRepairResult.fromRepair(startEpoch, accordRanges, true, true, true, false);
                                                ConsensusTableMigration.completedRepairJobHandler.onSuccess(new RepairResult(desc, null, repairResult));
                                            }).call();

        // Wait for everyone to get to where they are supposed to be
        pausedBeforeEnacting.call();
        i1PausedAfterEnacting.call();
        i2PausedAfterEnacting.call();
        // Unpause on 1 and 2 where we want them aware of the migration
        unpauseEnactment(i1);
        unpauseEnactment(i2);
        result.get();

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
