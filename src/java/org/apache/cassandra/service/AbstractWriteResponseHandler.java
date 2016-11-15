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
package org.apache.cassandra.service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.locator.InetAddressAndPorts;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public abstract class AbstractWriteResponseHandler<T> implements IAsyncCallbackWithFailure<T>
{
    protected static final Logger logger = LoggerFactory.getLogger( AbstractWriteResponseHandler.class );

    private final SimpleCondition condition = new SimpleCondition();
    protected final Keyspace keyspace;
    protected final Collection<InetAddressAndPorts> naturalEndpoints;
    public final ConsistencyLevel consistencyLevel;
    protected final Runnable callback;
    protected final Collection<InetAddressAndPorts> pendingEndpoints;
    protected final WriteType writeType;
    private static final AtomicIntegerFieldUpdater<AbstractWriteResponseHandler> failuresUpdater
        = AtomicIntegerFieldUpdater.newUpdater(AbstractWriteResponseHandler.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddressAndPorts, RequestFailureReason> failureReasonByEndpoint;
    private final long queryStartNanoTime;
    private volatile boolean supportsBackPressure = true;

    /**
     * @param callback A callback to be called when the write is successful.
     * @param queryStartNanoTime
     */
    protected AbstractWriteResponseHandler(Keyspace keyspace,
                                           Collection<InetAddressAndPorts> naturalEndpoints,
                                           Collection<InetAddressAndPorts> pendingEndpoints,
                                           ConsistencyLevel consistencyLevel,
                                           Runnable callback,
                                           WriteType writeType,
                                           long queryStartNanoTime)
    {
        this.keyspace = keyspace;
        this.pendingEndpoints = pendingEndpoints;
        this.consistencyLevel = consistencyLevel;
        this.naturalEndpoints = naturalEndpoints;
        this.callback = callback;
        this.writeType = writeType;
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        this.queryStartNanoTime = queryStartNanoTime;
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        long timeout = currentTimeout();

        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
        {
            int blockedFor = totalBlockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
            throw new WriteTimeoutException(writeType, consistencyLevel, acks, blockedFor);
        }

        if (totalBlockFor() + failures > totalEndpoints())
        {
            throw new WriteFailureException(consistencyLevel, ackCount(), totalBlockFor(), writeType, failureReasonByEndpoint);
        }
    }

    public final long currentTimeout()
    {
        long requestTimeout = writeType == WriteType.COUNTER
                              ? DatabaseDescriptor.getCounterWriteRpcTimeout()
                              : DatabaseDescriptor.getWriteRpcTimeout();
        return TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (System.nanoTime() - queryStartNanoTime);
    }

    /**
     * @return the minimum number of endpoints that must reply.
     */
    protected int totalBlockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return consistencyLevel.blockFor(keyspace) + pendingEndpoints.size();
    }

    /**
     * @return the total number of endpoints the request has been sent to.
     */
    protected int totalEndpoints()
    {
        return naturalEndpoints.size() + pendingEndpoints.size();
    }

    /**
     * @return true if the message counts towards the totalBlockFor() threshold
     */
    protected boolean waitingFor(InetAddressAndPorts from)
    {
        return true;
    }

    /**
     * @return number of responses received
     */
    protected abstract int ackCount();

    /** null message means "response from local write" */
    public abstract void response(MessageIn<T> msg);

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(keyspace, Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), isAlive));
    }

    protected void signal()
    {
        condition.signalAll();
        if (callback != null)
            callback.run();
    }

    @Override
    public void onFailure(InetAddressAndPorts from, RequestFailureReason failureReason)
    {
        logger.trace("Got failure from {}", from);

        int n = waitingFor(from)
              ? failuresUpdater.incrementAndGet(this)
              : failures;

        failureReasonByEndpoint.put(from, failureReason);

        if (totalBlockFor() + n > totalEndpoints())
            signal();
    }

    @Override
    public boolean supportsBackPressure()
    {
        return supportsBackPressure;
    }

    public void setSupportsBackPressure(boolean supportsBackPressure)
    {
        this.supportsBackPressure = supportsBackPressure;
    }
}
