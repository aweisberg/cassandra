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
package org.apache.cassandra.service.reads;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.locator.ReplicaLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.repair.BlockingReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

/**
 * Sends a read request to the replicas needed to satisfy a given ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against replica failure:
 * AlwaysSpeculatingReadExecutor will always send a request to one extra replica, while
 * SpeculatingReadExecutor will wait until it looks like the original request is in danger
 * of timing out before performing extra reads.
 */
public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected final ReadCommand command;
    private final ReplicaLayout.ForToken replicaLayout;
    protected final ReadRepair<EndpointsForToken, ReplicaLayout.ForToken> readRepair;
    protected final DigestResolver<EndpointsForToken, ReplicaLayout.ForToken> digestResolver;
    protected final ReadCallback<EndpointsForToken, ReplicaLayout.ForToken> handler;
    protected final TraceState traceState;
    protected final ColumnFamilyStore cfs;
    protected final long queryStartNanoTime;
    protected volatile PartitionIterator result = null;

    AbstractReadExecutor(ColumnFamilyStore cfs, ReadCommand command, ReplicaLayout.ForToken replicaLayout, long queryStartNanoTime)
    {
        this.command = command;
        this.replicaLayout = replicaLayout;
        this.readRepair = ReadRepair.create(command, replicaLayout, queryStartNanoTime);
        this.digestResolver = new DigestResolver<>(command, replicaLayout, readRepair, queryStartNanoTime);
        this.handler = new ReadCallback<>(digestResolver, replicaLayout.consistencyLevel().blockFor(replicaLayout.keyspace()), command, replicaLayout, queryStartNanoTime);
        this.cfs = cfs;
        this.traceState = Tracing.instance.get();
        this.queryStartNanoTime = queryStartNanoTime;


        // Set the digest version (if we request some digests). This is the smallest version amongst all our target replicas since new nodes
        // knows how to produce older digest but the reverse is not true.
        // TODO: we need this when talking with pre-3.0 nodes. So if we preserve the digest format moving forward, we can get rid of this once
        // we stop being compatible with pre-3.0 nodes.
        int digestVersion = MessagingService.current_version;
        for (Replica replica : replicaLayout.selected())
            digestVersion = Math.min(digestVersion, MessagingService.instance().getVersion(replica.endpoint()));
        command.setDigestVersion(digestVersion);
    }

    public DecoratedKey getKey()
    {
        Preconditions.checkState(command instanceof SinglePartitionReadCommand,
                                 "Can only get keys for SinglePartitionReadCommand");
        return ((SinglePartitionReadCommand) command).partitionKey();
    }

    public ReadRepair getReadRepair()
    {
        return readRepair;
    }

    protected void makeDataRequests(ReplicaCollection<?> replicas)
    {
        makeRequests(command, replicas);

    }

    protected void makeDigestRequests(ReplicaCollection<?> replicas)
    {
        // only send digest requests to full replicas, send data requests instead to the transient replicas
        makeRequests(command.copyAsDigestQuery(), replicas.filter(Replica::isFull));
        makeRequests(command, replicas.filter(Replica::isTransient));
    }

    private void makeRequests(ReadCommand readCommand, ReplicaCollection<?> replicas)
    {
        boolean hasLocalEndpoint = false;

        Preconditions.checkArgument(replicas.stream().allMatch(replica -> replica.isFull() || !readCommand.isDigestQuery()),
                                    "Can not send digest requests to transient replicas");
        for (Replica replica: replicas)
        {
            InetAddressAndPort endpoint = replica.endpoint();
            if (replica.isLocal())
            {
                hasLocalEndpoint = true;
                continue;
            }

            if (traceState != null)
                traceState.trace("reading {} from {}", readCommand.isDigestQuery() ? "digest" : "data", endpoint);
            MessageOut<ReadCommand> message = readCommand.createMessage();
            MessagingService.instance().sendRRWithFailure(message, endpoint, handler);
        }

        // We delay the local (potentially blocking) read till the end to avoid stalling remote requests.
        if (hasLocalEndpoint)
        {
            logger.trace("reading {} locally", readCommand.isDigestQuery() ? "digest" : "data");
            StageManager.getStage(Stage.READ).maybeExecuteImmediately(new LocalReadRunnable(command, handler));
        }
    }

    /**
     * Perform additional requests if it looks like the original will time out.  May block while it waits
     * to see if the original requests are answered first.
     */
    public abstract void maybeTryAdditionalReplicas();

    /**
     * send the initial set of requests
     */
    public abstract void executeAsync();

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadExecutor getReadExecutor(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, long queryStartNanoTime) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;

        // Endpoints for Token
        ReplicaLayout.ForToken replicaLayout = ReplicaLayout.forRead(keyspace, command.partitionKey().getToken(), consistencyLevel, retry);

        // Speculative retry is disabled *OR*
        // 11980: Disable speculative retry if using EACH_QUORUM in order to prevent miscounting DC responses
        if (retry.equals(NeverSpeculativeRetryPolicy.INSTANCE) || consistencyLevel == ConsistencyLevel.EACH_QUORUM)
            return new NeverSpeculatingReadExecutor(cfs, command, replicaLayout, queryStartNanoTime, false);

        // There are simply no extra replicas to speculate.
        // Handle this separately so it can log failed attempts to speculate due to lack of replicas
        if (consistencyLevel.blockFor(keyspace) == replicaLayout.all().size())
            return new NeverSpeculatingReadExecutor(cfs, command, replicaLayout, queryStartNanoTime, true);

        // If CL.ALL, upgrade to AlwaysSpeculating;
        // If We are going to contact every node anyway, ask for 2 full data requests instead of 1, for redundancy
        // (same amount of requests in total, but we turn 1 digest request into a full blown data request)
        if (replicaLayout.all().size() == replicaLayout.selected().size() || retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE))
        {
            return new AlwaysSpeculatingReadExecutor(cfs, command, replicaLayout, queryStartNanoTime);
        }
        else // PERCENTILE or CUSTOM.
            return new SpeculatingReadExecutor(cfs, command, replicaLayout, queryStartNanoTime);
    }

    /**
     *  Returns true if speculation should occur and if it should then block until it is time to
     *  send the speculative reads
     */
    boolean shouldSpeculateAndMaybeWait()
    {
        // no latency information, or we're overloaded
        if (cfs.sampleReadLatencyNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
            return false;

        return !handler.await(cfs.sampleReadLatencyNanos, TimeUnit.NANOSECONDS);
    }

    ReplicaLayout.ForToken replicaLayout()
    {
        return replicaLayout;
    }

    void onReadTimeout() {}

    public static class NeverSpeculatingReadExecutor extends AbstractReadExecutor
    {
        /**
         * If never speculating due to lack of replicas
         * log it is as a failure if it should have happened
         * but couldn't due to lack of replicas
         */
        private final boolean logFailedSpeculation;

        public NeverSpeculatingReadExecutor(ColumnFamilyStore cfs, ReadCommand command, ReplicaLayout.ForToken replicaLayout, long queryStartNanoTime, boolean logFailedSpeculation)
        {
            super(cfs, command, replicaLayout, queryStartNanoTime);
            this.logFailedSpeculation = logFailedSpeculation;
        }

        public void executeAsync()
        {
            makeDataRequests(replicaLayout().selected().subList(0, 1));
            if (replicaLayout().selected().size() > 1)
                makeDigestRequests(replicaLayout().selected().subList(1, replicaLayout().selected().size()));
        }

        public void maybeTryAdditionalReplicas()
        {
            if (shouldSpeculateAndMaybeWait() && logFailedSpeculation)
            {
                cfs.metric.speculativeInsufficientReplicas.inc();
            }
        }
    }

    static class SpeculatingReadExecutor extends AbstractReadExecutor
    {
        private volatile ReplicaLayout.ForToken replicaLayout;
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(ColumnFamilyStore cfs,
                                       ReadCommand command,
                                       ReplicaLayout.ForToken replicaLayout,
                                       long queryStartNanoTime)
        {
            super(cfs, command, replicaLayout, queryStartNanoTime);
            this.replicaLayout = replicaLayout;
        }

        public ReplicaLayout.ForToken replicaLayout()
        {
            return replicaLayout;
        }

        public void executeAsync()
        {
            EndpointsForToken initialReplicas = replicaLayout().selected();

            if (handler.blockfor < initialReplicas.size())
            {
                // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
                // preferred by the snitch, we do an extra data read to start with against a replica more
                // likely to reply; better to let RR fail than the entire query.
                makeDataRequests(initialReplicas.subList(0, 2));
                if (initialReplicas.size() > 2)
                    makeDigestRequests(initialReplicas.subList(2, initialReplicas.size()));
            }
            else
            {
                // not doing read repair; all replies are important, so it doesn't matter which nodes we
                // perform data reads against vs digest.
                makeDataRequests(initialReplicas.subList(0, 1));
                if (initialReplicas.size() > 1)
                    makeDigestRequests(initialReplicas.subList(1, initialReplicas.size()));
            }
        }

        public void maybeTryAdditionalReplicas()
        {
            if (shouldSpeculateAndMaybeWait())
            {
                //Handle speculation stats first in case the callback fires immediately
                speculated = true;
                replicaLayout = ReplicaLayout.forSpeculation(replicaLayout);
                cfs.metric.speculativeRetries.inc();
                // Could be waiting on the data, or on enough digests.
                Replica extraReplica = replicaLayout().selected().get(replicaLayout().selected().size() - 1);
                ReadCommand retryCommand = command;

                if (handler.resolver.isDataPresent() && extraReplica.isFull())
                    retryCommand = command.copyAsDigestQuery();

                if (traceState != null)
                    traceState.trace("speculating read retry on {}", extraReplica);
                logger.trace("speculating read retry on {}", extraReplica);
                MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(), extraReplica.endpoint(), handler);
            }
        }

        @Override
        void onReadTimeout()
        {
            //Shouldn't be possible to get here without first attempting to speculate even if the
            //timing is bad
            assert speculated;
            cfs.metric.speculativeFailedRetries.inc();
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor
    {
        public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs,
                                             ReadCommand command,
                                             ReplicaLayout.ForToken targetReplicas,
                                             long queryStartNanoTime)
        {
            super(cfs, command, targetReplicas, queryStartNanoTime);
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        @Override
        public void executeAsync()
        {
            makeDataRequests(replicaLayout().selected().subList(0, replicaLayout().selected().size() > 1 ? 2 : 1));
            if (replicaLayout().selected().size() > 2)
                makeDigestRequests(replicaLayout().selected().subList(2, replicaLayout().selected().size()));
            cfs.metric.speculativeRetries.inc();
        }

        @Override
        void onReadTimeout()
        {
            cfs.metric.speculativeFailedRetries.inc();
        }
    }

    public void setResult(PartitionIterator result)
    {
        Preconditions.checkState(this.result == null, "Result can only be set once");
        this.result = result;
    }

    /**
     * Wait for the CL to be satisfied by responses
     */
    public void awaitResponses() throws ReadTimeoutException
    {
        try
        {
            handler.awaitResults();
        }
        catch (ReadTimeoutException e)
        {
            try
            {
                onReadTimeout();
            }
            finally
            {
                throw e;
            }
        }

        // return immediately, or begin a read repair
        if (digestResolver.responsesMatch())
        {
            setResult(digestResolver.getData());
        }
        else
        {
            Tracing.trace("Digest mismatch: Mismatch for key {}", getKey());
            readRepair.startRepair(digestResolver, this::setResult);
        }
    }

    public void awaitReadRepair() throws ReadTimeoutException
    {
        try
        {
            readRepair.awaitReads();
        }
        catch (ReadTimeoutException e)
        {
            if (Tracing.isTracing())
                Tracing.trace("Timed out waiting on digest mismatch repair requests");
            else
                logger.trace("Timed out waiting on digest mismatch repair requests");
            // the caught exception here will have CL.ALL from the repair command,
            // not whatever CL the initial command was at (CASSANDRA-7947)
            throw new ReadTimeoutException(replicaLayout().consistencyLevel(), handler.blockfor - 1, handler.blockfor, true);
        }
    }

    boolean isDone()
    {
        return result != null;
    }

    public void maybeSendAdditionalDataRequests()
    {
        if (isDone())
            return;

        readRepair.maybeSendAdditionalReads();
    }

    public PartitionIterator getResult() throws ReadFailureException, ReadTimeoutException
    {
        Preconditions.checkState(result != null, "Result must be set first");
        return result;
    }
}
