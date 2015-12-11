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
package org.apache.cassandra.utils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Monitor backpressure and signal listeners when backpressure starts and stops
 */
public class BackpressureMonitor
{
    private static final Logger logger = LoggerFactory.getLogger(BackpressureMonitor.class);

    private final AtomicLong weight = new AtomicLong();
    private final ReentrantLock backpressureLock = new ReentrantLock(false);
    private final List<BackpressureListener> listeners = new CopyOnWriteArrayList<>();
    private final long onWeight;
    private final long offWeight;
    private boolean onBackpressure = false;


    public BackpressureMonitor(long onWeight, long offWeight)
    {
        this.onWeight = onWeight;
        this.offWeight = offWeight;
        new Thread()
        { public void run() { while (true) { try { Thread.sleep(2000);} catch (InterruptedException e) { throw new RuntimeException(); } logger.info("Current backpressure {}", weight.get());}}}.start();
    }

    public void addListener(BackpressureListener listener)
    {
        Preconditions.checkState(!listeners.contains(listener));
        listeners.add(listener);
    }

    /**
     * Signal the weight of backpressure has changed.
     *
     * The intended concurrency here is that state changes from on to off backpressure are serialized
     * by the lock, but that threads that don't create a state change will be able to proceed without
     * locking and without waiting for the backpressure signaling to complete.
     *
     * You can still run into trouble if two transitions queue up so there is still an incentive for
     * backpressure listeners to be quick about it.
     *
     */
    void backpressure(long weightChange)
    {
        if (weightChange == 0 | onWeight == Long.MAX_VALUE)
            return;

        while(true)
        {
            final long oldWeight = weight.get();
            final long newWeight = oldWeight + weightChange;

            if (newWeight < 0)
                throw new RuntimeException("Weight change from " + oldWeight + " to " + newWeight + " is invalid. Can't go negative.");

            //Isolated state transition from on backpressure to off required
            if (oldWeight >= offWeight && newWeight < offWeight)
            {
                if (backpressureLock.tryLock())
                {
                    try
                    {
                        if (weight.compareAndSet(oldWeight, newWeight) & onBackpressure)
                        {

                            logger.info ("Backpressure stopped old weight " + oldWeight + " new weight " + newWeight);
                            listeners.stream().forEach(l -> l.backpressureStateChange(false));
                            onBackpressure = false;
                            return;
                        }
                    }
                    finally
                    {
                        backpressureLock.unlock();
                    }
                }
                continue;
            }

            //Isolated state transition from off backpressure to on required
            if (oldWeight < onWeight && newWeight >= onWeight)
            {
                if (backpressureLock.tryLock())
                {
                    try
                    {
                        if (weight.compareAndSet(oldWeight, newWeight) ^ !onBackpressure)
                        {
                            logger.info ("Backpressure started old weight " + oldWeight + " new weight " + newWeight);
                            listeners.stream().forEach(l -> l.backpressureStateChange(true));
                            onBackpressure = true;
                            return;
                        }
                    }
                    finally
                    {
                        backpressureLock.unlock();
                    }
                }
                continue;
            }

            //No state transition, just update the weight
            if (weight.compareAndSet(oldWeight, newWeight))
                return;
        }
    }

    public void reset()
    {
        backpressureLock.lock();
        try
        {
            weight.set(0);
            listeners.stream().forEach(l -> l.backpressureStateChange(false));
            listeners.clear();
        }
        finally
        {
            backpressureLock.unlock();
        }
    }

    /**
     * Listen for backpressure signals from downstream and react accordingly
     */
    public interface BackpressureListener
    {
        void backpressureStateChange(boolean onBackpressure);
    }

    public interface WeightHolder extends AutoCloseable
    {
        public static final WeightHolder DUMMY = new DummyWeightHolder();
        /**
         * Easier to set the expected weight after finishing most message processing because at that point
         * the number of outstanding callbacks is known. This method incrementally adds weight and refcounts
         * for each outstanding callback.
         */
        public void addWeight(long additionalWeight);

        /**
         * Decrement the ref count, and if it reaches 0 reduce the weight of backpressure
         */
        public void decRef();

        public void close();
    }

    /**
     * Reference count something that has a certain weight. When the ref count reaches 0 return the weight.
     *
     * The refcount always starts at 1 so the weight can be incrementally calculated while another thread maybe be decRefing
     * at the same time. If no additional weight is added it is assumed the weight represented by this holder never
     * actually got stuck somewhere, say MessagingService and no callback was registered, so it isn't counted
     * for backpressure
     */
    public class WeightHolderImpl extends AtomicInteger implements WeightHolder
    {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private long weight;

        public WeightHolderImpl(long initialWeight)
        {
            super(1);
            Preconditions.checkArgument(initialWeight >= 0);
            weight = initialWeight;
        }

        public void addWeight(long additionalWeight)
        {
            Preconditions.checkArgument(additionalWeight >= 0);
            //Shouldn't add weight after it has been ref counted to 0
            Preconditions.checkState(get() > 0);
            weight += additionalWeight;
            incrementAndGet();
        }

        public void decRef()
        {
            Preconditions.checkState(weight >= 0);
            if (decrementAndGet() == 0)
            {
                long retval = weight;
                weight = -1;
                backpressure(-retval);
            }
        }

        public void close()
        {
            //The initial weight of a weight holder isn't counted, given how it's used with MessagingService
            if (get() == 1)
            {
                weight = -1;
                return;
            }
            backpressure(weight);
            decRef();
        }
    }

    public static class DummyWeightHolder implements WeightHolder
    {
        public void addWeight(long additionalWeight)
        {
            throw new UnsupportedOperationException();
        }

        public void decRef()
        {
            return;
        }

        public void close()
        {
            throw new UnsupportedOperationException();
        }
    }
}
