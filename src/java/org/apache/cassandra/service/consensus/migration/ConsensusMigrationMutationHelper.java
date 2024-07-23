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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Keys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy.WriteResponseHandlerWrapper;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnKeyRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperations;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Predicate.not;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.writeMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.writeMetricsForLevel;
import static org.apache.cassandra.service.StorageProxy.mutate;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.retry_new_protocol;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.getTableMetadata;
import static org.apache.cassandra.utils.Throwables.unchecked;

/**
 * Applying mutations can fail with RetryOnDifferentSystemException if a
 * mutation conflicts with a table and range that needs to be managed
 * transactionally. This impacts mutations, logged/unlogged batches, hints,and blocking read repair.
 *
 * This class contains the logic needed for managing these retry loops and splitting the mutations up
 */
public class ConsensusMigrationMutationHelper
{
    private static final Logger logger = LoggerFactory.getLogger(ConsensusMigrationMutationHelper.class);

    private static ConsistencyLevel consistencyLevelForCommit(ClusterMetadata cm, Collection<? extends IMutation> mutations, @Nullable ConsistencyLevel consistencyLevel)
    {
        // Null means no specific consistency behavior is required from Accord, it's functionally similar to ANY
        // if you aren't reading the result back via Accord
        if (consistencyLevel == null)
            return null;

        for (IMutation mutation : mutations)
        {
            for (TableId tableId : mutation.getTableIds())
            {
                TransactionalMode mode = getTableMetadata(cm, tableId).params.transactionalMode;
                // commitCLForStrategy should return either null or the supplied consistency level
                // in which case we will commit everything at that CL since Accord doesn't support per table
                // commit consistency
                ConsistencyLevel commitCL = mode.commitCLForStrategy(consistencyLevel, cm, tableId, mutation.key().getToken());
                if (commitCL != null)
                    return commitCL;
            }
        }
        return null;
    }

    public static <T extends IMutation> Pair<List<T>, List<T>> splitMutationsIntoAccordAndNormal(ClusterMetadata cm, List<T> mutations)
    {
        List<T> accordMutations = null;
        List<T> normalMutations = null;
        for (int i=0,mi=mutations.size(); i<mi; i++)
        {
            T mutation = mutations.get(i);
            Pair<T, T> accordAndNormalMutation = splitMutationIntoAccordAndNormal(mutation, cm);
            T accordMutation = accordAndNormalMutation.left;
            if (accordMutation != null)
            {
                if (accordMutations == null)
                    accordMutations = new ArrayList<>(Math.min(mutations.size(), 10));
                accordMutations.add(accordMutation);
            }
            T normalMutation = accordAndNormalMutation.right;
            if (normalMutation != null)
            {
                if (normalMutations == null)
                    normalMutations = new ArrayList<>(Math.min(mutations.size(), 10));
                normalMutations.add(normalMutation);
            }
        }
        return Pair.create(accordMutations, normalMutations);
    }

    public static <T extends IMutation> Pair<T, T> splitMutationIntoAccordAndNormal(T mutation, ClusterMetadata cm)
    {
        if (mutation.allowsPotentialTransactionConflicts())
            return Pair.create(null, mutation);

        Token token = mutation.key().getToken();
        Predicate<TableId> isAccordUpdate = tableId -> {
            TableMetadata tm = getTableMetadata(cm, tableId);
            if (tm == null)
                return false;
            return tokenShouldBeWrittenThroughAccord(cm, tm, token);
        };

        T accordMutation = (T)mutation.filter(isAccordUpdate);
        T normalMutation = (T)mutation.filter(not(isAccordUpdate));
        for (PartitionUpdate pu : mutation.getPartitionUpdates())
            checkState((accordMutation == null ? false : accordMutation.hasUpdateForTable(pu.metadata().id))
                       || (normalMutation == null ? false : normalMutation.hasUpdateForTable(pu.metadata().id)),
                       "All partition updates should still be present after splitting");
        return Pair.create(accordMutation, normalMutation);
    }

    public static Pair<List<? extends IMutation>, List<WriteResponseHandlerWrapper>> splitWrappersIntoAccordAndNormal(ClusterMetadata cm, List<WriteResponseHandlerWrapper> wrappers)
    {
        List<IMutation> accordMutations = null;
        List<WriteResponseHandlerWrapper> normalWrappers = null;
        for (int i=0,mi=wrappers.size(); i<mi; i++)
        {
            WriteResponseHandlerWrapper wrapper = wrappers.get(i);
            Pair<IMutation, WriteResponseHandlerWrapper> accordAndNormalMutation = splitWrapperIntoAccordAndNormal(wrapper, cm);
            IMutation accordMutation = accordAndNormalMutation.left;
            if (accordMutation != null)
            {
                if (accordMutations == null)
                    accordMutations = new ArrayList<>(Math.min(wrappers.size(), 10));
                accordMutations.add(accordMutation);
            }
            WriteResponseHandlerWrapper normalWrapper = accordAndNormalMutation.right;
            if (normalWrapper != null)
            {
                if (normalWrappers == null)
                    normalWrappers = new ArrayList<>(Math.min(wrappers.size(), 10));
                normalWrappers.add(normalWrapper);
            }
        }
        return Pair.create(accordMutations, normalWrappers);
    }

    private static Pair<IMutation, WriteResponseHandlerWrapper> splitWrapperIntoAccordAndNormal(WriteResponseHandlerWrapper wrapper, ClusterMetadata cm)
    {
        Mutation mutation = wrapper.mutation;
        if (mutation.allowsPotentialTransactionConflicts())
            return Pair.create(null, wrapper);

        Token token = mutation.key().getToken();
        Predicate<TableId> isAccordUpdate = tableId -> {
            TableMetadata tm = getTableMetadata(cm, tableId);
            if (tm == null)
                return false;
            return tokenShouldBeWrittenThroughAccord(cm, tm, token);
        };

        IMutation accordMutation = mutation.filter(isAccordUpdate);
        Mutation normalMutation = mutation.filter(not(isAccordUpdate));
        for (PartitionUpdate pu : mutation.getPartitionUpdates())
            checkState((accordMutation == null ? false : accordMutation.hasUpdateForTable(pu.metadata().id))
                       || (normalMutation == null ? false : normalMutation.hasUpdateForTable(pu.metadata().id)),
                       "All partition updates should still be present after splitting");
        if (accordMutation == null)
            return Pair.create(null, wrapper);
        if (normalMutation == null)
            return Pair.create( accordMutation, null);
        // Since this now depends on an Accord txn need to wait for that to complete first
        wrapper.handler.deferCleanup();
        return Pair.create(accordMutation, new WriteResponseHandlerWrapper(wrapper.handler, normalMutation));
    }

    public static Pair<TxnId, Future<TxnResult>> mutateWithAccordAsync(ClusterMetadata cm, Mutation mutation, @Nullable ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    {
        return mutateWithAccordAsync(cm, ImmutableList.of(mutation), consistencyLevel, queryStartNanoTime);
    }


    public static Pair<TxnId, Future<TxnResult>> mutateWithAccordAsync(ClusterMetadata cm, Collection<? extends IMutation> mutations, @Nullable ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    {
        int fragmentIndex = 0;
        List<TxnWrite.Fragment> fragments = new ArrayList<>(mutations.size());
        List<PartitionKey> partitionKeys = new ArrayList<>(mutations.size());
        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                PartitionKey pk = PartitionKey.of(update);
                partitionKeys.add(pk);
                fragments.add(new TxnWrite.Fragment(PartitionKey.of(update), fragmentIndex++, update, TxnReferenceOperations.empty()));
            }
        }
        // Potentially ignore commit consistency level if the strategy specifies accord and not migration
        ConsistencyLevel clForCommit = consistencyLevelForCommit(cm, mutations, consistencyLevel);
        TxnUpdate update = new TxnUpdate(fragments, TxnCondition.none(), clForCommit, true);
        Txn.InMemory txn = new Txn.InMemory(Keys.of(partitionKeys), TxnKeyRead.EMPTY, TxnQuery.NONE, update);
        IAccordService accordService = AccordService.instance();
        return accordService.coordinateAsync(txn, consistencyLevel, queryStartNanoTime);
    }

    public static void dispatchMutationsWithRetryOnDifferentSystem(List<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    {
        while (true)
        {
            ClusterMetadata cm = ClusterMetadata.current();
            try
            {
                // Keep attempting to route the mutations until they all succeed on the correct system
                // TODO (review): Metrics are going to double count when we split between the two systems
                // but they will be split across different accord/non-accord metrics
                Pair<List<IMutation>, List<IMutation>> accordAndNormal = splitMutationsIntoAccordAndNormal(cm, (List<IMutation>)mutations);
                List<? extends IMutation> accordMutations = accordAndNormal.left;
                // TODO this can race with migration to/from Accord and Accord doesn't know what range we are referring to or it just hasn't updated to the epoch we are aware of
                // client side retry can still succeed but the error could be exposed
                Pair<TxnId, Future<TxnResult>> accordResult = accordMutations != null ? mutateWithAccordAsync(cm, accordMutations, consistencyLevel, queryStartNanoTime) : null;
                List<? extends IMutation> normalMutations = accordAndNormal.right;

                Throwable failure = null;
                try
                {
                    if (normalMutations != null)
                        mutate(normalMutations, consistencyLevel, queryStartNanoTime);
                }
                catch (RetryOnDifferentSystemException | CoordinatorBehindException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                    failure = Throwables.merge(failure, e);
                }

                // Check if the Accord mutations succeeded asynchronously
                try
                {
                    if (accordResult != null)
                    {
                        IAccordService accord = AccordService.instance();
                        TxnResult.Kind kind = accord.getTxnResult(accordResult, true, consistencyLevel, queryStartNanoTime).kind();
                        if (kind == retry_new_protocol)
                            throw new RetryOnDifferentSystemException();
                    }
                }
                catch (RetryOnDifferentSystemException | CoordinatorBehindException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                    failure = Throwables.merge(failure, e);
                }

                if (failure != null)
                    throw unchecked(failure);
            }
            catch (RetryOnDifferentSystemException e)
            {
                writeMetrics.retryDifferentSystem.mark();
                writeMetricsForLevel(consistencyLevel).retryDifferentSystem.mark();
                logger.debug("Retrying mutations on different system because some mutations were misrouted");
                continue;
            }
            catch (CoordinatorBehindException e)
            {
                mutations.forEach(IMutation::clearCachedSerializationsForRetry);
                logger.debug("Retrying mutations now that coordinator has caught up to cluster metadata");
                continue;
            }
            break;
        }
    }

    public static void validateSafeToExecuteNonTransactionally(IMutation mutation) throws RetryOnDifferentSystemException
    {
        if (mutation.allowsPotentialTransactionConflicts())
            return;

        // System keyspaces are never managed by Accord
        if (SchemaConstants.isSystemKeyspace(mutation.getKeyspaceName()))
            return;

        ClusterMetadata cm = ClusterMetadata.current();

        DecoratedKey dk = mutation.key();
        // Check all the partition updates and if any can't be done return an error response
        // and the coordinator can retry with things correctly routed
        boolean throwRetryOnDifferentSystem = false;
        // Track CFS so we only mark each one once
        Set<TableId> markedColumnFamilies = null;
        for (PartitionUpdate pu : mutation.getPartitionUpdates())
        {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(pu.metadata().keyspace, pu.metadata().name);
            TableMetadata tm = getTableMetadata(cm, pu.metadata().id);
            if (tokenShouldBeWrittenThroughAccord(cm, tm, dk.getToken()))
            {
                throwRetryOnDifferentSystem = true;
                if (markedColumnFamilies == null)
                    markedColumnFamilies = new HashSet<>();
                if (markedColumnFamilies.add(cfs.getTableId()))
                    cfs.metric.mutationsRejectedOnWrongSystem.mark();
                logger.debug("Rejecting mutation on wrong system to table {}.{}", cfs.keyspace.getName(), cfs.name);
                Tracing.trace("Rejecting mutation on wrong system to table {}.{} token {}", cfs.keyspace.getName(), cfs.name, dk.getToken());
            }
        }
        if (throwRetryOnDifferentSystem)
            throw new RetryOnDifferentSystemException();
    }

//    public static boolean tokenShouldBeReadThroughAccord(@Nonnull ClusterMetadata cm, @Nonnull TableMetadata tm, @Nonnull Token token)
//    {
//        boolean transactionalModeReadsThroughAccord = tm.params.transactionalMode.readsThroughAccord;
//        TransactionalMigrationFromMode transactionalMigrationFromMode = tm.params.transactionalMigrationFrom;
//        boolean migrationFromReadsThroughAccord = transactionalMigrationFromMode.readsThroughAccord();
//        if (transactionalModeReadsThroughAccord && migrationFromReadsThroughAccord)
//            return true;
//
//
//    }

    public static boolean tokenShouldBeWrittenThroughAccord(@Nonnull ClusterMetadata cm, @Nonnull TableMetadata tm, @Nonnull Token token)
    {
        boolean transactionalModeWritesThroughAccord = tm.params.transactionalMode.writesThroughAccord;
        TransactionalMigrationFromMode transactionalMigrationFromMode = tm.params.transactionalMigrationFrom;
        boolean migrationFromWritesThroughAccord = transactionalMigrationFromMode.writesThroughAccord();
        if (transactionalModeWritesThroughAccord && migrationFromWritesThroughAccord)
            return true;

        // Could be migrating or could be completely migrated, if it's migrating check if the key for this mutation
        if (transactionalModeWritesThroughAccord || migrationFromWritesThroughAccord)
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tm.id);

            if (tms == null)
            {
                if (transactionalMigrationFromMode == TransactionalMigrationFromMode.none)
                    // There is no migration and no TMS so do what the schema says since no migration should be required
                    return transactionalModeWritesThroughAccord;
                else
                    // If we are migrating from something and there is no migration state the migration hasn't begun
                    // so continue to do what we are migrating from does until the range is marked as migrating
                    return migrationFromWritesThroughAccord;
            }

            // This logic is driven by the fact that Paxos is not picky about how data is written since it's txn recovery
            // is deterministic in the face of non-deterministic reads because consensus is agreeing on the writes that will be done to the database
            // Accord agrees on what computation will produce those writes and then asynchronously executes those computations, potentially multiple times
            // with different results if Accord reads non-transactionally written data that could be seen differently by different coordinators

            // If the current mode writes through Accord then we should always write though Accord for ranges managed by Accord.
            // Accord needs to do synchronous commit and respect the consistency level so that Accord will later be able to
            // read its own writes
            if (transactionalModeWritesThroughAccord)
                return tms.migratingAndMigratedRanges.intersects(token);

            // If we are migrating from a mode that used to write to Accord then any range that isn't migrating/migrated
            // should continue to write through Accord.
            // It's not completely symmetrical because Paxos is able to read Accord's writes by performing a single key barrier
            // and regular mutations will be able to do the same thing (needs to be added along with non-transactional reads)
            // This means that migrating ranges don't need to be written through Accord because we are running Paxos now
            // and not Accord. When migrating to Accord we need to do all the writes through Accord even if we aren't
            // reading through Accord so that repair + Accord metadata is sufficient for Accord to be able to read
            // safely and deterministically from any coordinator
            if (migrationFromWritesThroughAccord)
                return !tms.migratingAndMigratedRanges.intersects(token);
        }
        return false;
    }
}
