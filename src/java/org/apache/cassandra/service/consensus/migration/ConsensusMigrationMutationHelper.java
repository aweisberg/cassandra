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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.IAccordService.AsyncTxnResult;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperations;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Throwables;

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

    private static ConsistencyLevel consistencyLevelForCommit(Collection<? extends IMutation> mutations, @Nullable ConsistencyLevel consistencyLevel)
    {
        // Null means no specific consistency behavior is required from Accord, it's functionally similar to ANY
        // if you aren't reading the result back via Accord
        if (consistencyLevel == null)
            return null;

        for (IMutation mutation : mutations)
        {
            for (TableId tableId : mutation.getTableIds())
            {
                TransactionalMode mode = Schema.instance.getTableMetadata(tableId).params.transactionalMode;
                // commitCLForStrategy should return either null or the supplied consistency level
                // in which case we will commit everything at that CL since Accord doesn't support per table
                // commit consistency
                ConsistencyLevel commitCL = mode.commitCLForStrategy(consistencyLevel, tableId, mutation.key().getToken());
                if (commitCL != null)
                    return commitCL;
            }
        }
        return null;
    }

    /**
     * Result of splitting mutations across Accord and non-transactional boundaries
     */
    public static class SplitMutations<T extends IMutation> implements SplitConsumer<T>
    {
        @Nullable
        private List<T> accordMutations;

        @Nullable
        private List<T> normalMutations;

        private SplitMutations() {}

        public List<T> accordMutations()
        {
            return accordMutations;
        }

        public List<T> normalMutations()
        {
            return normalMutations;
        }

        @Override
        public void consume(@Nullable T accordMutation, @Nullable T normalMutation, List<T> mutations, int mutationIndex)
        {
            // Avoid allocating an ArrayList in common single mutation single system case
            if (mutations.size() == 1 && (accordMutation != null ^ normalMutation != null))
            {
                if (accordMutation != null)
                    accordMutations = mutations;
                else
                    normalMutations = mutations;
                return;
            }

            if (accordMutation != null)
            {
                if (accordMutations == null)
                    accordMutations = new ArrayList<>(Math.min(mutations.size(), 10));
                accordMutations.add(accordMutation);
            }
            if (normalMutation != null)
            {
                if (normalMutations == null)
                    normalMutations = new ArrayList<>(Math.min(mutations.size(), 10));
                normalMutations.add(normalMutation);
            }
        }
    }

    public interface SplitConsumer<T extends IMutation>
    {
        void consume(@Nullable T accordMutation, @Nullable T normalMutation, List<T> mutations, int mutationIndex);
    }

    public static <T extends IMutation, N> SplitMutations<T> splitMutationsIntoAccordAndNormal(ClusterMetadata cm, List<T> mutations)
    {
        SplitMutations<T> splitMutations = new SplitMutations<>();
        splitMutationsIntoAccordAndNormal(cm, mutations, splitMutations);
        return splitMutations;
    }

    public static <T extends IMutation> void splitMutationsIntoAccordAndNormal(ClusterMetadata cm, List<T> mutations, SplitConsumer<T> splitConsumer)
    {
        for (int i=0,mi=mutations.size(); i<mi; i++)
        {
            SplitMutation<T> splitMutation = splitMutationIntoAccordAndNormal(mutations.get(i), cm);
            splitConsumer.consume(splitMutation.accordMutation, splitMutation.normalMutation, mutations, i);
        }
    }

    /**
     * Result of splitting a mutation across Accord and non-transactional boundaries
     */
    public static class SplitMutation<T extends IMutation>
    {
        @Nullable
        public final T accordMutation;
        @Nullable
        public final T normalMutation;

        public SplitMutation(@Nullable T accordMutation, @Nullable T normalMutation)
        {
            this.accordMutation = accordMutation;
            this.normalMutation = normalMutation;
        }
    }

    public static <T extends IMutation> SplitMutation<T> splitMutationIntoAccordAndNormal(T mutation, ClusterMetadata cm)
    {
        if (mutation.allowsPotentialTransactionConflicts())
            return new SplitMutation<>(null, mutation);

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
        return new SplitMutation(accordMutation, normalMutation);
    }

    public static AsyncTxnResult mutateWithAccordAsync(ClusterMetadata cm, Mutation mutation, @Nullable ConsistencyLevel consistencyLevel, long queryStartNanoTime)
    {
        return mutateWithAccordAsync(cm, ImmutableList.of(mutation), consistencyLevel, queryStartNanoTime);
    }

    public static AsyncTxnResult mutateWithAccordAsync(ClusterMetadata cm, Collection<? extends IMutation> mutations, @Nullable ConsistencyLevel consistencyLevel, long queryStartNanoTime)
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
        ConsistencyLevel clForCommit = consistencyLevelForCommit(mutations, consistencyLevel);
        TxnUpdate update = new TxnUpdate(fragments, TxnCondition.none(), clForCommit, true);
        Txn.InMemory txn = new Txn.InMemory(Keys.of(partitionKeys), TxnRead.EMPTY, TxnQuery.NONE, update);
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
                SplitMutations splitMutations = splitMutationsIntoAccordAndNormal(cm, (List<IMutation>)mutations);
                List<? extends IMutation> accordMutations = splitMutations.accordMutations();
                AsyncTxnResult accordResult = accordMutations != null ? mutateWithAccordAsync(cm, accordMutations, consistencyLevel, queryStartNanoTime) : null;
                List<? extends IMutation> normalMutations = splitMutations.normalMutations();

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
                writeMetrics.mutationRetriedOnDifferentSystem.mark();
                writeMetricsForLevel(consistencyLevel).mutationRetriedOnDifferentSystem.mark();
                logger.debug("Retrying mutations on different system because some mutations were misrouted");
                continue;
            }
            catch (CoordinatorBehindException e)
            {
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
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(pu.metadata().id);
            TableMetadata tm = cfs.metadata();
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
