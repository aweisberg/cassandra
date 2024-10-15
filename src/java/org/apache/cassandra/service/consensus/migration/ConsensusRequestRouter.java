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

package org.apache.cassandra.service.consensus.migration;

import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.getConsensusMigratedAt;
import static org.apache.cassandra.service.consensus.migration.ConsensusMigrationTarget.paxos;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.accord;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV1;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV2;

/**
 * Helper class to decide where to route a request that requires consensus, migrating a key if necessary
 * before rerouting.
 *
 * This router has to be used for all SERIAL reads and writes to ensure the correct operation of Paxos/Acocrd during migration
 * and for all non-SERIAL reads because non-SERIAL reads may end up being routed to Accord and Accord needs CRR to manage
 * any key migrations that need to be performed
 */
public class ConsensusRequestRouter
{
    public enum ConsensusRoutingDecision
    {
        paxosV1,
        paxosV2,
        accord,
    }

    public static volatile ConsensusRequestRouter instance = new ConsensusRequestRouter();

    @VisibleForTesting
    public static void setInstance(ConsensusRequestRouter testInstance)
    {
        instance = testInstance;
    }

    @VisibleForTesting
    public static void resetInstance()
    {
        instance = new ConsensusRequestRouter();
    }

    protected ConsensusRequestRouter() {}

    ConsensusRoutingDecision decisionFor(TransactionalMode transactionalMode)
    {
        if (transactionalMode.accordIsEnabled)
            return accord;

        return pickPaxos();
    }

    /*
     * Accord never handles local tables, but if the table doesn't exist then we need to generate the correct
     * InvalidRequestException.
     */
    private static TableMetadata metadata(ClusterMetadata cm, String keyspace, String table)
    {
        Optional<KeyspaceMetadata> ksm = cm.schema.maybeGetKeyspaceMetadata(keyspace);
        if (ksm.isEmpty())
        {
            // It's a non-distributed table which is fine, but we want to error if it doesn't exist
            // We should never actually reach here unless there is a race with dropping the table
            Keyspaces localKeyspaces = Schema.instance.localKeyspaces();
            KeyspaceMetadata ksm2 = localKeyspaces.getNullable(keyspace);
            if (ksm2 == null)
                throw new InvalidRequestException("Keyspace " + keyspace + " does not exist");
            // Explicitly including views in case they get used in non-distributed tables
            TableMetadata tbm2 = ksm2.getTableOrViewNullable(table);
            if (tbm2 == null)
                throw new InvalidRequestException("Table " + keyspace + "." + table + " does not exist");
            return null;
        }
        TableMetadata tbm = ksm.get().getTableNullable(table);
        if (tbm == null)
            throw new InvalidRequestException("Table " + keyspace + "." + table + " does not exist");

        return tbm;
    }

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull ClusterMetadata cm, @Nonnull DecoratedKey key, @Nonnull String keyspace, @Nonnull String table, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {
        TableMetadata metadata = metadata(cm, keyspace, table);

        // Non-distributed tables always take the Paxos path
        if (metadata == null)
            return pickPaxos();
        return routeAndMaybeMigrate(cm, metadata, key, consistencyLevel, requestTime, timeoutNanos, isForWrite);
    }

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull ClusterMetadata cm, @Nonnull DecoratedKey key, @Nonnull TableId tableId, ConsistencyLevel consistencyLevel,  Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {
        TableMetadata metadata = getTableMetadata(cm, tableId);
        // Non-distributed tables always take the Paxos path
        if (metadata == null)
            pickPaxos();
        return routeAndMaybeMigrate(cm, metadata, key, consistencyLevel, requestTime, timeoutNanos, isForWrite);
    }

    public static TableMetadata getTableMetadata(ClusterMetadata cm, TableId tableId)
    {
        TableMetadata tm = cm.schema.getTableMetadata(tableId);
        if (tm == null)
        {
            // It's a non-distributed table which is fine, but we want to error if it doesn't exist
            // We should never actually reach here unless there is a race with dropping the table
            Keyspaces localKeyspaces = Schema.instance.localKeyspaces();
            TableMetadata tm2 = localKeyspaces.getTableOrViewNullable(tableId);
            if (tm2 == null)
                throw new InvalidRequestException("Table with id " + tableId + " does not exist");
            return null;
        }
        return tm;
    }

    protected ConsensusRoutingDecision routeAndMaybeMigrate(ClusterMetadata cm, @Nonnull TableMetadata tmd, @Nonnull DecoratedKey key, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {

        if (!tmd.params.transactionalMigrationFrom.isMigrating())
            return decisionFor(tmd.params.transactionalMode);

        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tmd.id);
        if (tms == null)
            return decisionFor(tmd.params.transactionalMigrationFrom.from);

        Token token = key.getToken();
        if (tms.migratedRanges.intersects(token))
            return pickMigrated(tms.targetProtocol);

        if (tms.migratingRanges.intersects(token))
            return pickBasedOnKeyMigrationStatus(cm, tmd, tms, key, consistencyLevel, requestTime, timeoutNanos, isForWrite);

        // It's not migrated so infer the protocol from the target
        return pickNotMigrated(tms.targetProtocol);
    }

    /**
     * If the key was already migrated then we can pick the target protocol otherwise
     * we have to run a repair operation on the key to migrate it.
     */
    private static ConsensusRoutingDecision pickBasedOnKeyMigrationStatus(ClusterMetadata cm, TableMetadata tmd, TableMigrationState tms, DecoratedKey key, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {
        checkState(pickPaxos() != paxosV1, "Can't migrate from PaxosV1 to anything");

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tmd.id);
        if (cfs == null)
            throw new InvalidRequestException("Can't route consensus request to nonexistent CFS %s.%s".format(tmd.keyspace, tmd.name));

        // Migration to accord has two phases for each range, in the first phase we can't do key migration because Accord
        // can't safely read until the range has had its data repaired so Paxos continues to be used for all reads
        // and writes
        Token token = key.getToken();
        if (tms.targetProtocol == ConsensusMigrationTarget.accord && tms.repairPendingRanges.intersects(token))
            return pickPaxos();

        // If it is locally replicated we can check our local migration state to see if it was already migrated
        EndpointsForToken naturalReplicas = ReplicaLayout.forNonLocalStrategyTokenRead(cm, cfs.keyspace.getMetadata(), token);
        boolean isLocallyReplicated = naturalReplicas.lookup(FBUtilities.getBroadcastAddressAndPort()) != null;
        if (isLocallyReplicated)
        {
            ConsensusMigratedAt consensusMigratedAt = getConsensusMigratedAt(tms.tableId, key);
            // Check that key migration that was performed satisfies the requirements of the current in flight migration
            // for the range
            // Be aware that for Accord->Paxos the cache only tells us if the key was repaired locally
            // This ends up still being safe because every single Paxos read (in a migrating range) during migration will check
            // locally to see if repair is necessary
            if (consensusMigratedAt != null && tms.satisfiedByKeyMigrationAtEpoch(key, consensusMigratedAt))
                return pickMigrated(tms.targetProtocol);

            if (tms.targetProtocol == paxos)
            {
                // Run the Accord barrier txn now so replicas don't start independent
                // barrier transactions to accomplish the migration
                // They still might need to go through the fast local path for barrier txns
                // at each replica, but they won't create their own txn since we created it here
                ConsensusKeyMigrationState.repairKeyAccord(key, tms.tableId, tms.minMigrationEpoch(token).getEpoch(), requestTime, true, isForWrite);
                return paxosV2;
            }
            // Fall through for repairKeyPaxos
        }

        // If it's not locally replicated then:
        // Accord -> Paxos - Paxos will ask Accord to migrate in the read at each replica if necessary
        // Paxos -> Accord - Paxos needs to be repaired before Accord runs so do it here
        if (tms.targetProtocol == paxos)
            // TODO (important): Why are these two cases paxosV2 instead of `pickPaxos`?
            // Because we only supported PaxosV2 for migration?
            // Eventually we want to support both so just use pickPaxos and error out on migration from paxosV1 elsewhere?
            return paxosV2;
        else
        {
            if (tms.accordSafeToReadRanges.intersects(key.getToken()))
                // Should exit exceptionally if the repair is not done
                ConsensusKeyMigrationState.repairKeyPaxos(naturalReplicas, cm.epoch, key, cfs, consistencyLevel, requestTime, timeoutNanos, isLocallyReplicated, isForWrite);
            else
                return pickPaxos();
        }

        return pickMigrated(tms.targetProtocol);
    }

    // Allows tests to inject specific responses
    public boolean isKeyInMigratingOrMigratedRangeDuringPaxosBegin(TableId tableId, DecoratedKey key)
    {
        return isKeyInMigratingOrMigratedRangeFromPaxos(tableId, key);
    }

    // Allows tests to inject specific responses
    public boolean isKeyInMigratingOrMigratedRangeDuringPaxosAccept(TableId tableId, DecoratedKey key)
    {
        return isKeyInMigratingOrMigratedRangeFromPaxos(tableId, key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state.
     */
    public boolean isKeyInMigratingOrMigratedRangeFromPaxos(TableId tableId, DecoratedKey key)
    {
        TableMigrationState tms = ClusterMetadata.current().consensusMigrationState.tableStates.get(tableId);
        // No state means no migration for this table
        if (tms == null)
            return false;

        // We assume that key migration was already performed and it's safe to execute this on Paxos
        if (tms.targetProtocol == ConsensusMigrationTarget.paxos)
            return false;

        Token token = key.getToken();
        // Migration from Paxos to Accord has two phases and in the first phase we continue to run Paxos
        // until the data has been repaired for the range so that Accord can safely read it after Paxos key migration
        if (tms.repairPendingRanges.intersects(token))
            return false;
        // The coordinator will need to retry either on Accord if they are trying
        // to propose their own value, or by setting the consensus migration epoch to recover an incomplete transaction
        if (tms.migratingAndMigratedRanges.intersects(token))
            return true;

        return false;
    }

    public boolean isKeyManagedByAccordForReadAndWrite(ClusterMetadata cm, TableId tableId, DecoratedKey key)
    {
        return isKeyManagedByAccordForReadAndWrite(getTableMetadata(cm, tableId),
                                                   cm.consensusMigrationState.tableStates.get(tableId),
                                                   key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state. It just assumes that the key migration has already been done.
     *
     * This version is for is full read write transactions
     */
    public boolean isKeyManagedByAccordForReadAndWrite(TableMetadata metadata, TableMigrationState tms, DecoratedKey key)
    {
        TransactionalMode transactionalMode = metadata.params.transactionalMode;
        TransactionalMigrationFromMode migrationFrom = metadata.params.transactionalMigrationFrom;
        Token token = key.getToken();

        if (migrationFrom.isMigrating())
            checkState(tms != null, "Can't have migration in progress without tms");

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;
            // Accord can only read/write the key if it is in a safe to read (repaired) range
            if (tms.accordSafeToReadRanges.intersects(token))
                return true;
        }
        else
        {
            // Once the migration starts only barriers are allowed to run for the key in Accord
            if (migrationFrom.migratingFromAccord() && !tms.migratingAndMigratedRanges.intersects(token))
                return true;
        }

        return false;
    }

    public boolean isKeyManagedByAccordForWrite(ClusterMetadata cm, TableId tableId, DecoratedKey key)
    {
        return isKeyManagedByAccordForWrite(getTableMetadata(cm, tableId),
                                            cm.consensusMigrationState.tableStates.get(tableId),
                                            key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state. It just assumes that the key migration has already been done.
     *
     * This version is for writes through Accord before Accord is able to safely read.
     */
    public boolean isKeyManagedByAccordForWrite(TableMetadata metadata, TableMigrationState tms, DecoratedKey key)
    {
        TransactionalMode transactionalMode = metadata.params.transactionalMode;
        TransactionalMigrationFromMode migrationFrom = metadata.params.transactionalMigrationFrom;
        Token token = key.getToken();

        if (migrationFrom.isMigrating())
            checkState(tms != null, "Can't have migration in progress without tms");

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;
            // Accord can blind write to the key even if it isn't safe to read from it so use migratingAndMigratedRanges
            if (tms.migratingAndMigratedRanges.intersects(token))
                return true;
        }
        else
        {
            // Once the migration away from Accord starts only barriers are allowed to run for the key in Accord
            // and this method isn't used for barriers
            if (migrationFrom.migratingFromAccord() && !tms.migratingAndMigratedRanges.intersects(token))
                return true;
        }

        return false;
    }

    private static ConsensusRoutingDecision pickMigrated(ConsensusMigrationTarget targetProtocol)
    {
        if (targetProtocol.equals(ConsensusMigrationTarget.accord))
            return accord;
        else
            return pickPaxos();
    }

    private static ConsensusRoutingDecision pickNotMigrated(ConsensusMigrationTarget targetProtocol)
    {
        if (targetProtocol.equals(ConsensusMigrationTarget.accord))
            return pickPaxos();
        else
            return accord;
    }

    private static ConsensusRoutingDecision pickPaxos()
    {
        return Paxos.useV2() ? paxosV2 : paxosV1;
    }

    public static void validateSafeToReadNonTransactionally(ReadCommand command)
    {
        if (command.allowsPotentialTxnConflicts())
            return;

        String keyspace = command.metadata().keyspace;
        // System keyspaces are never managed by Accord
        if (SchemaConstants.isSystemKeyspace(keyspace))
            return;

        // Local keyspaces are never managed by Accord
        if (Schema.instance.localKeyspaces().containsKeyspace(keyspace))
            return;

        ClusterMetadata cm = ClusterMetadata.current();
        TableId tableId = command.metadata().id;
        TableMetadata tableMetadata = getTableMetadata(cm, tableId);
        // Null for local tables
        if (tableMetadata == null)
            return;

        TransactionalMode transactionalMode = tableMetadata.params.transactionalMode;
        TransactionalMigrationFromMode transactionalMigrationFromMode = tableMetadata.params.transactionalMigrationFrom;
        if (!transactionalMode.nonSerialReadsThroughAccord && !transactionalMigrationFromMode.nonSerialReadsThroughAccord())
            return;

        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);

        // Null with a transaction mode that reads through Accord indicates a completed migration or table created
        // to use Accdord initially
        if (tms == null)
        {
            checkState(transactionalMigrationFromMode == TransactionalMigrationFromMode.off);
            if (transactionalMode.nonSerialReadsThroughAccord)
                throw new RetryOnDifferentSystemException();
        }

        boolean isExclusivelyReadableFromAccord;
        if (command.isRangeRequest())
            isExclusivelyReadableFromAccord = isBoundsExclusivelyManagedByAccordForRead(transactionalMode, transactionalMigrationFromMode, tms, ((PartitionRangeReadCommand)command).dataRange().keyRange());
        else
            isExclusivelyReadableFromAccord = isTokenExclusivelyManagedByAccordForRead(transactionalMode, transactionalMigrationFromMode, tms, ((SinglePartitionReadCommand)command).partitionKey().getToken());

        if (isExclusivelyReadableFromAccord)
            throw new RetryOnDifferentSystemException();
    }

    private static boolean isTokenExclusivelyManagedByAccordForRead(@Nonnull TransactionalMode transactionalMode,
                                                                     @Nonnull TransactionalMigrationFromMode migrationFrom,
                                                                     @Nonnull TableMigrationState tms,
                                                                     @Nonnull Token token)
    {
        checkNotNull(transactionalMode, "transactionalMode is null");
        checkNotNull(migrationFrom, "migrationFrom is null");
        checkNotNull(tms, "tms (TableMigrationState) is null");
        checkNotNull(token, "bounds is null");

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;

            // Accord is exclusive once the range is fully migrated to Accord, but possible to read from safely
            // when accordSafeToReadRanges covers the entire bound
            if (tms.migratedRanges.intersects(token))
                return true;
        }
        else
        {
            // Once the migration starts only barriers are allowed to run for the key in Accord
            if (migrationFrom.migratingFromAccord() && !tms.migratingAndMigratedRanges.intersects(token))
                return true;
        }

        return false;
    }

    private static boolean isBoundsExclusivelyManagedByAccordForRead(@Nonnull TransactionalMode transactionalMode,
                                                                     @Nonnull TransactionalMigrationFromMode migrationFrom,
                                                                     @Nonnull TableMigrationState tms,
                                                                     @Nonnull AbstractBounds<PartitionPosition> bounds)
    {
        checkNotNull(transactionalMode, "transactionalMode is null");
        checkNotNull(migrationFrom, "migrationFrom is null");
        checkNotNull(tms, "tms (TableMigrationState) is null");
        checkNotNull(bounds, "bounds is null");

        BiPredicate<AbstractBounds<PartitionPosition>, NormalizedRanges<Token>> intersects = (testBounds, testRanges) -> {
            // TODO (nicetohave): Efficiency of this intersection
            for (org.apache.cassandra.dht.Range<Token> range : testRanges)
            {
                if (Range.makeRowRange(range).intersects(testBounds))
                    return true;
            }
            return false;
        };

        if (bounds.left.getToken().equals(bounds.right.getToken()) && !bounds.inclusiveLeft() && bounds.inclusiveRight())
        {
            return isTokenExclusivelyManagedByAccordForRead(transactionalMode, migrationFrom, tms, bounds.left.getToken());
        }

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;

            // Accord is exclusive once the range is fully migrated to Accord, but possible to read from safely
            // when accordSafeToReadRanges covers the entire bound
            if (intersects.test(bounds, tms.migratedRanges))
                return true;
        }
        else
        {
            // Once the migration starts only barriers are allowed to run for the key in Accord
            if (migrationFrom.migratingFromAccord() && !intersects.test(bounds, tms.migratingAndMigratedRanges))
                return true;
        }

        return false;
    }

    /**
     * Result of splitting mutations across Accord and non-transactional boundaries
     */
    public static class SplitReads
    {
        @Nullable
        public final SinglePartitionReadCommand.Group accordReads;

        @Nullable
        public final SinglePartitionReadCommand.Group normalReads;

        private SplitReads(SinglePartitionReadCommand.Group accordReads, SinglePartitionReadCommand.Group normalReads)
        {
            this.accordReads = accordReads;
            this.normalReads = normalReads;
        }
    }

    public static SplitReads splitReadsIntoAccordAndNormal(ClusterMetadata cm, SinglePartitionReadCommand.Group read)
    {
        List<SinglePartitionReadCommand> accordReads;
        List<SinglePartitionReadCommand> normalReads;

        for (SinglePartitionReadCommand command : read.queries)
        {

        }
    }

    public static boolean tokenShouldBeReadThroughAccord(@Nonnull ClusterMetadata cm,
                                                         @Nonnull TableId tableId,
                                                         @Nonnull Token token)
    {
        TableMetadata tm = getTableMetadata(cm, tableId);
        if (tm == null)
            return false;

        boolean transactionalModeReadsThroughAccord = tm.params.transactionalMode.nonSerialReadsThroughAccord;
        TransactionalMigrationFromMode transactionalMigrationFromMode = tm.params.transactionalMigrationFrom;
        boolean migrationFromReadsThroughAccord = transactionalMigrationFromMode.nonSerialReadsThroughAccord();
        if (transactionalModeReadsThroughAccord && migrationFromReadsThroughAccord)
            return true;

        // Could be migrating or could be completely migrated, if it's migrating check if the key for this mutation
        if (transactionalModeReadsThroughAccord || migrationFromReadsThroughAccord)
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tm.id);

            if (tms == null)
            {
                if (transactionalMigrationFromMode == TransactionalMigrationFromMode.none)
                    // There is no migration and no TMS so do what the schema says since no migration should be required
                    return transactionalModeReadsThroughAccord;
                else
                    // If we are migrating from something and there is no migration state the migration hasn't begun
                    // so continue to do what we are migrating from does until the range is marked as migrating
                    return migrationFromReadsThroughAccord;
            }

            // This logic is driven by the fact that Paxos is not picky about how data is written since it's txn recovery
            // is deterministic in the face of non-deterministic reads because consensus is agreeing on the writes that will be done to the database
            // Accord agrees on what computation will produce those writes and then asynchronously executes those computations, potentially multiple times
            // with different results if Accord reads non-transactionally written data that could be seen differently by different coordinators

            // If the current mode writes through Accord then we should always write though Accord for ranges managed by Accord.
            // Accord needs to do synchronous commit and respect the consistency level so that Accord will later be able to
            // read its own writes
            if (transactionalModeReadsThroughAccord)
                return tms.accordSafeToReadRanges.intersects(token);

            // If we are migrating from a mode that used to write to Accord then any range that isn't migrating/migrated
            // should continue to write through Accord.
            // It's not completely symmetrical because Paxos is able to read Accord's writes by performing a single key barrier
            // and regular mutations will be able to do the same thing (needs to be added along with non-transactional reads)
            // This means that migrating ranges don't need to be written through Accord because we are running Paxos now
            // and not Accord. When migrating to Accord we need to do all the writes through Accord even if we aren't
            // reading through Accord so that repair + Accord metadata is sufficient for Accord to be able to read
            // safely and deterministically from any coordinator
            if (migrationFromReadsThroughAccord)
                return !tms.migratingAndMigratedRanges.intersects(token);
        }
        return false;
    }
}
