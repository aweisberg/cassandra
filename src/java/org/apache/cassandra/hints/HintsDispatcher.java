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
package org.apache.cassandra.hints;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.TxnId;
import org.apache.cassandra.concurrent.DebuggableTask.RunnableDebuggableTask;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.IAccordService.AsyncTxnResult;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper.SplitMutation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.FAILURE;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.INTERRUPTED;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.SUCCESS;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.TIMEOUT;
import static org.apache.cassandra.hints.HintsService.RETRY_ON_DIFFERENT_SYSTEM_UUID;
import static org.apache.cassandra.metrics.HintsServiceMetrics.ACCORD_HINT_ENDPOINT;
import static org.apache.cassandra.metrics.HintsServiceMetrics.updateDelayMetrics;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.retry_new_protocol;
import static org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper.mutateWithAccordAsync;
import static org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper.splitMutationIntoAccordAndNormal;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

/**
 * Dispatches a single hints file to a specified node in a batched manner.
 *
 * Uses either {@link HintMessage.Encoded} - when dispatching hints into a node with the same messaging version as the hints file,
 * or {@link HintMessage}, when conversion is required.
 */
final class HintsDispatcher implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatcher.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private enum Action { CONTINUE, ABORT }

    private final HintsReader reader;
    final UUID hostId;

    @Nullable
    final InetAddressAndPort address;
    private final int messagingVersion;
    private final BooleanSupplier abortRequested;

    private InputPosition currentPagePosition;

    // Hints from the batch log that were attempted on Accord don't have a list of hosts that need hinting
    // since Accord doesn't expose that on failure. If Accord no longer manages the range for this hint then we need
    // to send the hint to all replicas after the page succeeds
    private final Queue<Hint> hintsNeedingRehinting = new LinkedList<>();

    private HintsDispatcher(HintsReader reader, UUID hostId, @Nullable InetAddressAndPort address, int messagingVersion, BooleanSupplier abortRequested)
    {
        checkArgument(address != null ^ hostId.equals(RETRY_ON_DIFFERENT_SYSTEM_UUID), "address must be nonnull or hostId must be " + RETRY_ON_DIFFERENT_SYSTEM_UUID);
        currentPagePosition = null;

        this.reader = reader;
        this.hostId = hostId;
        this.address = address;
        this.messagingVersion = messagingVersion;
        this.abortRequested = abortRequested;
    }

    static HintsDispatcher create(File file, RateLimiter rateLimiter, @Nullable InetAddressAndPort address, UUID hostId, BooleanSupplier abortRequested)
    {
        int messagingVersion = address == null ? MessagingService.current_version : MessagingService.instance().versions.get(address);
        HintsDispatcher dispatcher = new HintsDispatcher(HintsReader.open(file, rateLimiter), hostId, address, messagingVersion, abortRequested);
        HintDiagnostics.dispatcherCreated(dispatcher);
        return dispatcher;
    }

    public void close()
    {
        HintDiagnostics.dispatcherClosed(this);
        reader.close();
    }

    String destination()
    {
        return address == null ? "RETRY_ON_DIFFERENT_SYSTEM" : address.toString();
    }

    void seek(InputPosition position)
    {
        reader.seek(position);
    }

    /**
     * @return whether or not dispatch completed entirely and successfully
     */
    boolean dispatch()
    {
        for (HintsReader.Page page : reader)
        {
            currentPagePosition = page.position;
            if (dispatch(page) != Action.CONTINUE)
                return false;
        }

        return true;
    }

    /**
     * @return offset of the first non-delivered page
     */
    InputPosition dispatchPosition()
    {
        return currentPagePosition;
    }


    // retry in case of a timeout; stop in case of a failure, host going down, or delivery paused
    private Action dispatch(HintsReader.Page page)
    {
        HintDiagnostics.dispatchPage(this);
        return sendHintsAndAwait(page);
    }

    private Action sendHintsAndAwait(HintsReader.Page page)
    {
        try
        {
            return doSendHintsAndAwait(page);
        }
        finally
        {
            hintsNeedingRehinting.clear();
        }
    }

    private Action doSendHintsAndAwait(HintsReader.Page page)
    {
        Collection<Callback> callbacks = new ArrayList<>();

        /*
         * If hints file messaging version matches the version of the target host, we'll use the optimised path -
         * skipping the redundant decoding/encoding cycle of the already encoded hint.
         *
         * If that is not the case, we'll need to perform conversion to a newer (or an older) format, and decoding the hint
         * is an unavoidable intermediate step.
         *
         * If Accord is enabled or these hints are from the batchlog and were originally attempted on Accord then
         * we also need to decode so we can route the Hint contents appropriately.
         */
        Action action = reader.descriptor().messagingVersion() == messagingVersion && !DatabaseDescriptor.getAccordTransactionsEnabled() && !hostId.equals(RETRY_ON_DIFFERENT_SYSTEM_UUID)
                      ? sendHints(page.buffersIterator(), callbacks, this::sendEncodedHint)
                      : sendHints(page.hintsIterator(), callbacks, this::sendHint);

        if (action == Action.ABORT)
            return action;

        long success = 0, failures = 0, timeouts = 0;
        for (Callback cb : callbacks)
        {
            Callback.Outcome outcome = cb.await();
            if (outcome == Callback.Outcome.SUCCESS) success++;
            else if (outcome == Callback.Outcome.FAILURE) failures++;
            else if (outcome == Callback.Outcome.TIMEOUT) timeouts++;
        }

        updateMetrics(success, failures, timeouts);

        if (failures > 0 || timeouts > 0)
        {
            HintDiagnostics.pageFailureResult(this, success, failures, timeouts);
            return Action.ABORT;
        }
        else
        {
            HintDiagnostics.pageSuccessResult(this, success, failures, timeouts);
            rehintHintsNeedingRehinting();
            return Action.CONTINUE;
        }
    }

    private void rehintHintsNeedingRehinting()
    {
        ClusterMetadata cm = ClusterMetadata.current();
        Hint hint;
        while ((hint = hintsNeedingRehinting.poll()) != null)
        {
            HintsService.instance.writeForAllReplicas(hint);
            Mutation mutation = hint.mutation;
            // Also may need to apply locally because it's possible this is from the batchlog
            // and we never applied it locally
            // TODO (review): Additional error handling necessary? Hints are lossy
            DataPlacement dataPlacement = cm.placements.get(cm.schema.getKeyspace(mutation.getKeyspaceName()).getMetadata().params.replication);
            VersionedEndpoints.ForToken forToken = dataPlacement.writes.forToken(mutation.key().getToken());
            Replica self = forToken.get().selfIfPresent();
            if (self != null)
            {
                Stage.MUTATION.maybeExecuteImmediately(new RunnableDebuggableTask()
                {
                    private final long approxCreationTimeNanos = MonotonicClock.Global.approxTime.now();
                    private volatile long approxStartTimeNanos;

                    @Override
                    public void run()
                    {
                        approxStartTimeNanos = MonotonicClock.Global.approxTime.now();
                        mutation.apply();
                    }

                    @Override
                    public long creationTimeNanos()
                    {
                        return approxCreationTimeNanos;
                    }

                    @Override
                    public long startTimeNanos()
                    {
                        return approxStartTimeNanos;
                    }

                    @Override
                    public String description()
                    {
                        return "HintsService rehinting Accord txn";
                    }
                });
            }
        }

    }

    private void updateMetrics(long success, long failures, long timeouts)
    {
        HintsServiceMetrics.hintsSucceeded.mark(success);
        HintsServiceMetrics.hintsFailed.mark(failures);
        HintsServiceMetrics.hintsTimedOut.mark(timeouts);
    }

    /*
     * Sending hints in compatibility mode.
     */

    private <T> Action sendHints(Iterator<T> hints, Collection<Callback> callbacks, Function<T, Callback> sendFunction)
    {
        while (hints.hasNext())
        {
            if (abortRequested.getAsBoolean())
            {
                HintDiagnostics.abortRequested(this);
                return Action.ABORT;
            }
            callbacks.add(sendFunction.apply(hints.next()));
        }
        return Action.CONTINUE;
    }

    // TODO (review): Could add the loop, but how often will it be needed?
    // While this could loop if splitting the mutation misroutes there isn't a strong need because Hints will retry anyways
    // It will cause hint delivery for this endpoint to pause until it is rescheduled again
    // Given the structure of page at a time delivery it's also a little tricky because we would really need to iterate over the entire page
    // again only applying the hints that ended up needing to be re-routed
    private Callback sendHint(Hint hint)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        SplitHint splitHint = splitHintIntoAccordAndNormal(cm, hint);
        Mutation accordHintMutation = splitHint.accordMutation;
        long txnStartNanoTime = 0;
        AsyncTxnResult accordTxnResult = null;
        if (accordHintMutation != null)
        {
            txnStartNanoTime = Clock.Global.nanoTime();
            accordTxnResult = accordHintMutation != null ? mutateWithAccordAsync(cm, accordHintMutation, null, txnStartNanoTime) : null;
        }

        Hint normalHint = splitHint.normalHint;
        Callback callback = new Callback(address, hint.creationTime, txnStartNanoTime, accordTxnResult);
        if (normalHint != null)
        {
            // We had a hint that was supposed to be done on Accord for the batch log (otherwise address would be non-null),
            // but Accord no longer manages that table/range and now we don't know which nodes (if any) are missing the Mutation.
            // Convert them to per replica hints *after* all the hints in this page have been applied so we can be reasonably sure
            // this page isn't going to be played again thus avoiding any futher amplification from the same hint being
            // replayed and repeatedly converted to per replica hints
            if (address == null)
            {
                checkState(hostId.equals(RETRY_ON_DIFFERENT_SYSTEM_UUID), "If there is no address to send the hint to then the host ID should be BATCHLOG_ACCORD_HINT_UUID");
                callback.onResponse(null);
                hintsNeedingRehinting.add(normalHint);
            }
            else
            {
                Message<?> message = Message.out(HINT_REQ, new HintMessage(hostId, normalHint));
                MessagingService.instance().sendWithCallback(message, address, callback);
            }
        }
        else
        {
            // Don't wait for a normal response that will never come since no hints were sent
            callback.onResponse(null);
        }

        return callback;
    }

    /**
     * Result of splitting a hint across Accord and non-transactional boundaries
     */
    private class SplitHint
    {
        private final Mutation accordMutation;
        private final Hint normalHint;

        public SplitHint(Mutation accordMutation, Hint normalHint)
        {
            this.accordMutation = accordMutation;
            this.normalHint = normalHint;
        }
    }

    private SplitHint splitHintIntoAccordAndNormal(ClusterMetadata cm, Hint hint)
    {
        SplitMutation<Mutation> splitMutation = splitMutationIntoAccordAndNormal(hint.mutation, cm);
        if (splitMutation.accordMutation == null)
            return new SplitHint(null, hint);
        if (splitMutation.normalMutation == null)
            return new SplitHint(splitMutation.accordMutation, null);
        Hint normalHint = Hint.create(splitMutation.normalMutation, hint.creationTime, splitMutation.normalMutation.smallestGCGS());
        return new SplitHint(splitMutation.accordMutation, normalHint);
    }

    /*
     * Sending hints in raw mode.
     */

    private Callback sendEncodedHint(ByteBuffer hint)
    {
        HintMessage.Encoded message = new HintMessage.Encoded(hostId, hint, messagingVersion);
        Callback callback = new Callback(address, message.getHintCreationTime());
        MessagingService.instance().sendWithCallback(Message.out(HINT_REQ, message), address, callback);
        return callback;
    }

    static final class Callback implements RequestCallback, Runnable
    {
        enum Outcome { SUCCESS, TIMEOUT, FAILURE, INTERRUPTED }

        private final long start = approxTime.now();
        private final Condition condition = newOneTimeCondition();
        private Outcome normalOutcome;
        private Outcome accordOutcome;
        @Nullable
        private final InetAddressAndPort to;
        private final long hintCreationNanoTime;
        private final long accordTxnStartNanos;
        private final AsyncTxnResult accordTxnResult;

        private Callback(@Nonnull InetAddressAndPort to, long hintCreationTimeMillisSinceEpoch)
        {
            this(to, hintCreationTimeMillisSinceEpoch, -1, null);
        }

        private Callback(@Nullable InetAddressAndPort to, long hintCreationTimeMillisSinceEpoch, long accordTxnStartNanos, @Nullable AsyncTxnResult accordTxnResult)
        {
            this.to = to != null ? to : ACCORD_HINT_ENDPOINT;
            this.hintCreationNanoTime = approxTime.translate().fromMillisSinceEpoch(hintCreationTimeMillisSinceEpoch);
            this.accordTxnStartNanos = accordTxnStartNanos;
            this.accordTxnResult = accordTxnResult;
            if (accordTxnResult != null)
                accordTxnResult.addListener(this, ImmediateExecutor.INSTANCE);
            else
                accordOutcome = SUCCESS;
        }

        Outcome await()
        {
            boolean timedOut;
            try
            {
                timedOut = !condition.awaitUntil(HINT_REQ.expiresAtNanos(start));
            }
            catch (InterruptedException e)
            {
                logger.warn("Hint dispatch was interrupted", e);
                return INTERRUPTED;
            }
            normalOutcome = timedOut ? TIMEOUT : normalOutcome;

            return outcome();
        }

        private Outcome outcome()
        {
            checkState((normalOutcome != null && accordOutcome != null) || (normalOutcome != SUCCESS || accordOutcome != SUCCESS), "Outcome for both normal and accord hint delivery should be known");
            if (normalOutcome == TIMEOUT || accordOutcome == TIMEOUT)
                return TIMEOUT;
            if (normalOutcome == FAILURE || accordOutcome == FAILURE)
                return FAILURE;
            checkState(normalOutcome == SUCCESS && accordOutcome == SUCCESS, "Hint delivery should have been successful");
            return SUCCESS;
        }

        private synchronized void maybeSignal()
        {
            if ((normalOutcome != null && accordOutcome != null) || normalOutcome == FAILURE || accordOutcome == FAILURE)
            {
                updateDelayMetrics(to, approxTime.now() - this.hintCreationNanoTime);
                condition.signalAll();
            }
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failureMessage)
        {
            normalOutcome = FAILURE;
            maybeSignal();
        }

        @Override
        public void onResponse(Message msg)
        {
            normalOutcome = SUCCESS;
            maybeSignal();
        }

        @Override
        public void run()
        {
            try
            {
                IAccordService accord = AccordService.instance();
                TxnResult.Kind kind = accord.getTxnResult(accordTxnResult, true, null, accordTxnStartNanos).kind();
                if (kind == retry_new_protocol)
                    throw new RetryOnDifferentSystemException();
                accordOutcome = SUCCESS;
            }
            catch (Exception e)
            {
                accordOutcome = e instanceof WriteTimeoutException ? TIMEOUT : FAILURE;
                String msg = "Accord hint delivery transaction failed";
                if (noSpamLogger.getStatement(msg).shouldLog(Clock.Global.nanoTime()))
                    logger.error(msg, e);
            }
            maybeSignal();
        }
    }
}
