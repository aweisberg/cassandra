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
package org.apache.cassandra.net;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;
import org.xerial.snappy.SnappyOutputStream;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

public class OutboundTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final String PREFIX = Config.PROPERTY_PREFIX;

    /*
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final String INTRADC_TCP_NODELAY_PROPERTY = PREFIX + "OTC_INTRADC_TCP_NODELAY";
    private static final boolean INTRADC_TCP_NODELAY = Boolean.valueOf(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    /*
     * How often to pull a new timestamp from the system.
     */
    private static final String TIMESTAMP_UPDATE_INTERVAL_PROPERTY = PREFIX + "OTC_TIMESTAMP_UPDATE_INTERVAL";
    private static final long TIMESTAMP_UPDATE_INTERVAL = Long.getLong(TIMESTAMP_UPDATE_INTERVAL_PROPERTY, 10000);

    private static volatile long TIMESTAMP_BASE[] = new long[] { System.currentTimeMillis(), System.nanoTime() };

    @VisibleForTesting
    static final Object TIMESTAMP_UPDATE = new Object();

    /*
     * System.currentTimeMillis() is 25 nanoseconds. This is 2 nanoseconds (maybe) according to JMH.
     * Faster than calling both currentTimeMillis() and nanoTime() for every outbound message.
     *
     * There is also the issue of how scalable nanoTime() and currentTimeMillis() are which is a moving target.
     *
     * These timestamps don't order with System.currentTimeMillis() because currentTimeMillis() can tick over
     * before this one does. I have seen it behind by as much as 2 milliseconds.
     */
    @VisibleForTesting
    static final long convertNanoTimeToCurrentTimeMillis(long nanoTime)
    {
        final long timestampBase[] = TIMESTAMP_BASE;
        return timestampBase[0] + TimeUnit.NANOSECONDS.toMillis(nanoTime - timestampBase[1]);
    }

    /*
     * Size of buffer in output stream
     */
    private static final String BUFFER_SIZE_PROPERTY = PREFIX + "OTC_BUFFER_SIZE";
    private static final int BUFFER_SIZE = Integer.getInteger(BUFFER_SIZE_PROPERTY, 1024 * 64);

    /*
     * How many microseconds to wait for coalescing. For fixed strategy this is the amount of time after the first
     * messgae is received before it will be sent with any accompanying messages. For moving average this is the
     * maximum amount of time that will be waited as well as the interval at which messages must arrive on average
     * for coalescing to be enabled.
     */
    private static final String COALESCING_WINDOW_PROPERTY = PREFIX + "OTC_COALESCING_WINDOW_MICROSECONDS";
    private static final int COALESCING_WINDOW = Integer.getInteger(COALESCING_WINDOW_PROPERTY, 200);

    /*
     * Strategy to use for Coalescing. Can be fixed, or movingaverage. Setting is case and leading/trailing
     * whitespace insensitive.
     */
    private static final String COALESCING_STRATEGY_PROPERTY = PREFIX + "OTC_COALESCING_STRATEGY";
    private static final String COALESCING_STRATEGY = System.getProperty(COALESCING_STRATEGY_PROPERTY, "TIMEHORIZON").trim().toUpperCase();

    /*
     * Log debug information at info level about what the average is and when coalescing is enabled/disabled
     */
    private static final String DEBUG_COALESCING_PROPERTY = PREFIX + "OTC_DEBUG_COALESCING";
    private static final boolean DEBUG_COALESCING = Boolean.getBoolean(DEBUG_COALESCING_PROPERTY) | logger.isDebugEnabled();

    private static void parkLoop(long nanos)
    {
        long now = System.nanoTime();
        final long timer = now + nanos;
        do
        {
            LockSupport.parkNanos(timer - now);
        }
        while (timer - (now = System.nanoTime()) > nanos / 16);
    }

    @VisibleForTesting
    interface CoalescingStrategy
    {

        /*
         * Drain from the input blocking queue to the output list up to outSize elements
         *
         * The coalescing strategy may choose to park the current thread if it thinks it will
         * be able to produce an output list with more elements
         */
        void coalesce(BlockingQueue<QueuedMessage> input, List<QueuedMessage> out, int outSize) throws InterruptedException;
    }


    @VisibleForTesting
    static class TimeHorizonMovingAverageCoalescingStrategy implements CoalescingStrategy
    {
        // for now we'll just use 64ms per bucket; this can be made configurable, but results in ~1s for 16 samples
        private static final int INDEX_SHIFT = 26;
        private static final long BUCKET_INTERVAL = 16L << 26;
        private static final int BUCKET_COUNT = 16;
        private static final long INTERVAL = BUCKET_INTERVAL * BUCKET_COUNT;
        private static final long MEASURED_INTERVAL = BUCKET_INTERVAL * (BUCKET_COUNT - 1);

        // the minimum timestamp we will now accept updates for; only moves forwards, never backwards
        private long epoch = System.nanoTime();
        // the buckets, each following on from epoch; the measurements run from ix(epoch) to ix(epoch - 1)
        // ix(epoch-1) is a partial result, that is never actually part of the calculation, and most updates
        // are expected to hit this bucket
        private final int samples[] = new int[BUCKET_COUNT];
        private long sum = 0;
        private final long maxCoalesceWindow;

        public TimeHorizonMovingAverageCoalescingStrategy(int maxCoalesceWindow)
        {
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            sum = 0;
            new Thread() {
                @Override
                public void run() {
                    while (true) {
                        try
                        {
                            Thread.sleep(5000);
                        }
                        catch (InterruptedException e)
                        {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        shouldSampleAverage = true;
                    }
                }
            }.start();
        }

        private void logSample(long nanos)
        {
            long epoch = this.epoch;
            long delta = nanos - epoch;
            if (delta < 0)
                // have to simply ignore, but would be a bit crazy to get such reordering
                return;

            if (delta > INTERVAL)
                epoch = rollepoch(delta, epoch, nanos);

            int ix = ix(nanos);
            samples[ix]++;

            // if we've updated an old bucket, we need to update the sum to match
            if (ix != ix(epoch - 1))
                sum++;
        }

        private long averageGap()
        {
            if (sum == 0)
                return Integer.MAX_VALUE;
            return MEASURED_INTERVAL / sum;
        }

        public boolean maybeSleep(int messages)
        {
            // only sleep if we can expect to double the number of messages we're sending in the time interval
            long sleep = messages * averageGap();
            if (sleep > maxCoalesceWindow)
                return false;

            // assume we receive as many messages as we expect; apply the same logic to the future batch:
            // expect twice as many messages to consider sleeping for "another" interval; this basically translates
            // to doubling our sleep period until we exceed our max sleep window
            while (sleep * 2 < maxCoalesceWindow)
                sleep *= 2;
            parkLoop(sleep);
            return true;
        }

        // this sample extends past the end of the range we cover, so rollover
        private long rollepoch(long delta, long epoch, long nanos)
        {
            if (delta > 2 * INTERVAL)
            {
                // this sample is more than twice our interval ahead, so just clear our counters completely
                epoch = epoch(nanos);
                sum = 0;
                Arrays.fill(samples, 0);
            }
            else
            {
                // ix(epoch - 1) => last index; this is our partial result bucket, so we add this to the sum
                sum += samples[ix(epoch - 1)];
                // then we roll forwards, clearing buckets, until our interval covers the new sample time
                for (int i = 0 ; epoch + INTERVAL < nanos; i++)
                {
                    int index = ix(epoch);
                    sum -= samples[index];
                    samples[index] = 0;
                    epoch += BUCKET_INTERVAL;
                }
            }
            // store the new epoch
            this.epoch = epoch;
            return epoch;
        }

        private long epoch(long latestNanos)
        {
            return (latestNanos - INTERVAL) & (BUCKET_INTERVAL - 1);
        }

        private int ix(long nanos)
        {
            return (int) ((nanos >>> INDEX_SHIFT) & 15);
        }

        volatile boolean shouldSampleAverage = false;

        @Override
        public void coalesce(BlockingQueue<QueuedMessage> input, List<QueuedMessage> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0) {
                out.add(input.take());
                input.drainTo(out, outSize - out.size());
            }

            for (QueuedMessage qm : out) {
                logSample(qm.timestampNanos);
            }

            if (shouldSampleAverage) {
                logger.info("Coalescing average " + TimeUnit.NANOSECONDS.toMicros(averageGap()));
                shouldSampleAverage = false;
            }

            int count = out.size();
            if (maybeSleep(count)) {
                input.drainTo(out, outSize - out.size());
                int prevCount = count;
                count = out.size();
                for (int  i = prevCount; i < count; i++)
                    logSample(out.get(i).timestampNanos);
            }
        }
    }

    /*
     * Start coalescing by sleeping if the moving average is < the requested window.
     * The actual time spent waiting to coalesce will be the min( window, moving average * 2)
     * The actual amount of time spent waiting can be greater then the window. For instance
     * observed time spent coalescing was 400 microseconds with the window set to 200 in one benchmark.
     */
    @VisibleForTesting
    static class MovingAverageCoalescingStrategy implements CoalescingStrategy
    {
        private final int samples[] = new int[16];
        private long lastSample = 0;
        private int index = 0;
        private long sum = 0;

        private final long maxCoalesceWindow;

        private long coalesceDecision = -1;

        public MovingAverageCoalescingStrategy(int maxCoalesceWindow)
        {
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            for (int ii = 0; ii < samples.length; ii++)
                samples[ii] = Integer.MAX_VALUE;
            sum = Integer.MAX_VALUE * (long)samples.length;
        }

        private long logSample(int value)
        {
            sum -= samples[index];
            sum += value;
            samples[index] = value;
            index++;
            index = index & ((1 << 4) - 1);
            return sum / 16;
        }

        private long notifyOfSample(long sample)
        {
            if (sample > lastSample)
            {
                final int delta = (int)(Math.min(Integer.MAX_VALUE, sample - lastSample));
                lastSample = sample;
                return logSample(delta);
            }
            else
            {
                return logSample(1);
            }
        }

        @Override
        public void coalesce(BlockingQueue<QueuedMessage> input, List<QueuedMessage> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0) {
                out.add(input.take());
            }

            long average = notifyOfSample(out.get(0).timestampNanos);

            if (DEBUG_COALESCING && ThreadLocalRandom.current().nextDouble() < .000001)
                logger.info("Coalescing average " + TimeUnit.NANOSECONDS.toMicros(average));

            if (average < maxCoalesceWindow)
            {
                if (coalesceDecision == -1 || average > coalesceDecision)
                {
                    coalesceDecision = Math.min(maxCoalesceWindow, average * 2);
                    if (DEBUG_COALESCING)
                        logger.info("Enabling coalescing average " + TimeUnit.NANOSECONDS.toMicros(average));
                }

                parkLoop(coalesceDecision);

                input.drainTo(out, outSize - out.size());
                for (int ii = 1; ii < out.size(); ii++) {
                    notifyOfSample(out.get(ii).timestampNanos);
                }

                return;
            }

            if (DEBUG_COALESCING && coalesceDecision != -1)
                logger.info("Disabling coalescing average " + TimeUnit.NANOSECONDS.toMicros(average));

            coalesceDecision = -1;

            input.drainTo(out, outSize - out.size());
            for (int ii = 1; ii < out.size(); ii++) {
                notifyOfSample(out.get(ii).timestampNanos);
            }
        }
    }

    /*
     * A fixed strategy as a backup in case MovingAverage or TimeHorizongMovingAverage fails in some scenario
     */
    @VisibleForTesting
    static class FixedCoalescingStrategy implements CoalescingStrategy
    {
        private final long coalesceWindow;

        public FixedCoalescingStrategy(int coalesceWindowMicros)
        {
            coalesceWindow = TimeUnit.MICROSECONDS.toNanos(coalesceWindowMicros);
        }

        @Override
        public void coalesce(BlockingQueue<QueuedMessage> input, List<QueuedMessage> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0) {
                out.add(input.take());
            }

            parkLoop(coalesceWindow);

            input.drainTo(out, outSize - out.size());
        }
    }

    /*
     * A coalesscing strategy that just returns all currently available elements
     */
    @VisibleForTesting
    static class DisabledCoalescingStrategy implements CoalescingStrategy
    {
        @Override
        public void coalesce(BlockingQueue<QueuedMessage> input, List<QueuedMessage> out,  int outSize) throws InterruptedException
        {
            if (input.drainTo(out, outSize) == 0) {
                out.add(input.take());
                input.drainTo(out, outSize - out.size());
            }
        }
    }

    @VisibleForTesting
    static class DisabledBrokenCoalescingStrategy implements CoalescingStrategy
    {
        @Override
        public void coalesce(BlockingQueue<QueuedMessage> input, List<QueuedMessage> out,  int outSize) throws InterruptedException
        {
            out.add(input.take());
        }
    }

    private static CoalescingStrategy newCoalescingStrategy()
    {
        System.out.println("Creating new coalescing strategy for " + COALESCING_STRATEGY);
        System.err.println("Creating new coalescing strategy for " + COALESCING_STRATEGY);
        logger.info("Creating new coalescing strategy for " + COALESCING_STRATEGY);
        switch(COALESCING_STRATEGY)
        {
        case "MOVINGAVERAGE":
            return new MovingAverageCoalescingStrategy(COALESCING_WINDOW);
        case "FIXED":
            return new FixedCoalescingStrategy(COALESCING_WINDOW);
        case "TIMEHORIZON":
            return new TimeHorizonMovingAverageCoalescingStrategy(COALESCING_WINDOW);
        case "DISABLED":
            return new DisabledCoalescingStrategy();
        case "DISABLED_BROKEN":
            return new DisabledBrokenCoalescingStrategy();
            default:
                throw new Error("Unrecognized coalescing strategy " + COALESCING_STRATEGY);
        }
    }

    static {
        switch (COALESCING_STRATEGY) {
        case "MOVINGAVERAGE":
        case "FIXED":
        case "TIMEHORIZON":
        case "DISABLED":
        case "DISABLED_BROKEN":
            break;
            default:
                throw new ExceptionInInitializerError(
                        "Unrecognized coalescing strategy provide via " + COALESCING_STRATEGY_PROPERTY +
                        ": " + COALESCING_STRATEGY);

        }

        if (COALESCING_WINDOW < 0)
            throw new ExceptionInInitializerError(
                    "Value provided for coalescing window via " + COALESCING_WINDOW_PROPERTY +
                    " must be greather than 0: " + COALESCING_WINDOW);

        //Pick up updates from NTP periodically
        Thread t = new Thread("OutboundTcpConnection time updater")
        {
            @Override
            public void run()
            {
                while (true)
                {
                    try
                    {
                        synchronized (TIMESTAMP_UPDATE)
                        {
                            TIMESTAMP_UPDATE.wait(TIMESTAMP_UPDATE_INTERVAL);
                        }
                    }
                    catch (InterruptedException e)
                    {
                        return;
                    }

                    TIMESTAMP_BASE = new long[] {
                            Math.max(TIMESTAMP_BASE[0], System.currentTimeMillis()),
                            Math.max(TIMESTAMP_BASE[1], System.nanoTime()) };
                }
            }
        };
        t.setDaemon(true);
        t.start();
    }

    private static final MessageOut CLOSE_SENTINEL = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
    private volatile boolean isStopped = false;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    public static final int WAIT_FOR_VERSION_MAX_TIME = 5000;
    private static final int NO_VERSION = Integer.MIN_VALUE;

    static final int LZ4_HASH_SEED = 0x9747b28c;

    private final BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<>();

    private final OutboundTcpConnectionPool poolReference;

    private final CoalescingStrategy cs = newCoalescingStrategy();
    private DataOutputStreamPlus out;
    private Socket socket;
    private volatile long completed;
    private final AtomicLong dropped = new AtomicLong();
    private volatile int currentMsgBufferCount = 0;
    private int targetVersion;

    public static final Semaphore logTimestamps = new Semaphore(1);

    public OutboundTcpConnection(OutboundTcpConnectionPool pool)
    {
        super("WRITE-" + pool.endPoint());
        this.poolReference = pool;
    }

    private static boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    public void enqueue(MessageOut<?> message, int id)
    {
        if (backlog.size() > 1024)
            expireMessages();
        try
        {
            backlog.put(new QueuedMessage(message, id));
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    void closeSocket(boolean destroyThread)
    {
        backlog.clear();
        isStopped = destroyThread; // Exit loop to stop the thread
        enqueue(CLOSE_SENTINEL, -1);
    }

    void softCloseSocket()
    {
        enqueue(CLOSE_SENTINEL, -1);
    }

    public int getTargetVersion()
    {
        return targetVersion;
    }

    public void run()
    {
        ByteBuffer logBuffer = null;
        RandomAccessFile ras = null;
        if (logTimestamps.tryAcquire()) {
            new File("/tmp/sillylog").delete();
            try {
                ras = new RandomAccessFile("/tmp/sillylog", "rw");
                logBuffer = ras.getChannel().map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
                logBuffer.order(ByteOrder.LITTLE_ENDIAN);
                logBuffer.putLong(0);
            } catch (Exception e) {
                logger.error("ouch", e);
            }
        }


        final int drainedMessageSize = 128;
        // keeping list (batch) size small for now; that way we don't have an unbounded array (that we never resize)
        final List<QueuedMessage> drainedMessages = new ArrayList<>(drainedMessageSize);

        outer:
        while (true)
        {
            try {
                cs.coalesce(backlog, drainedMessages, drainedMessageSize);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }

            currentMsgBufferCount = drainedMessages.size();

            int count = drainedMessages.size();
            //The timestamp of the first message has already been provided to the coalescing strategy
            //so skip logging it.
            for (QueuedMessage qm : drainedMessages)
            {
                if (logBuffer != null) {
                    logBuffer.putLong(0, logBuffer.getLong(0) + 1);
                    logBuffer.putLong(qm.timestampNanos);
                }
                try
                {
                    MessageOut<?> m = qm.message;
                    if (m == CLOSE_SENTINEL)
                    {
                        disconnect();
                        if (isStopped)
                            break outer;
                        continue;
                    }

                    if (qm.isTimedOut(TimeUnit.MILLISECONDS.toNanos(m.getTimeout()), System.nanoTime()))
                        dropped.incrementAndGet();
                    else if (socket != null || connect())
                        writeConnected(qm, count == 1 && backlog.isEmpty());
                    else
                        // clear out the queue, else gossip messages back up.
                        backlog.clear();
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    // really shouldn't get here, as exception handling in writeConnected() is reasonably robust
                    // but we want to catch anything bad we don't drop the messages in the current batch
                    logger.error("error processing a message intended for {}", poolReference.endPoint(), e);
                }
                currentMsgBufferCount = --count;
            }
            drainedMessages.clear();
        }
    }

    public int getPendingMessages()
    {
        return backlog.size() + currentMsgBufferCount;
    }

    public long getCompletedMesssages()
    {
        return completed;
    }

    public long getDroppedMessages()
    {
        return dropped.get();
    }

    private boolean shouldCompressConnection()
    {
        // assumes version >= 1.2
        return DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all
               || (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc && !isLocalDC(poolReference.endPoint()));
    }

    private void writeConnected(QueuedMessage qm, boolean flush)
    {
        try
        {
            byte[] sessionBytes = qm.message.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance.get(sessionId);
                String message = String.format("Sending message to %s", poolReference.endPoint());
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    byte[] traceTypeBytes = qm.message.parameters.get(Tracing.TRACE_TYPE);
                    Tracing.TraceType traceType = traceTypeBytes == null ? Tracing.TraceType.QUERY : Tracing.TraceType.deserialize(traceTypeBytes[0]);
                    TraceState.trace(ByteBuffer.wrap(sessionBytes), message, -1, traceType.getTTL(), null);
                }
                else
                {
                    state.trace(message);
                    if (qm.message.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }

            writeInternal(qm.message, qm.id, convertNanoTimeToCurrentTimeMillis(qm.timestampNanos));

            completed++;
            if (flush)
                out.flush();
        }
        catch (Exception e)
        {
            disconnect();
            if (e instanceof IOException)
            {
                if (logger.isDebugEnabled())
                    logger.debug("error writing to {}", poolReference.endPoint(), e);

                // if the message was important, such as a repair acknowledgement, put it back on the queue
                // to retry after re-connecting.  See CASSANDRA-5393
                if (qm.shouldRetry())
                {
                    try
                    {
                        backlog.put(new RetriedQueuedMessage(qm));
                    }
                    catch (InterruptedException e1)
                    {
                        throw new AssertionError(e1);
                    }
                }
            }
            else
            {
                // Non IO exceptions are likely a programming error so let's not silence them
                logger.error("error writing to {}", poolReference.endPoint(), e);
            }
        }
    }

    private void writeInternal(MessageOut message, int id, long timestamp) throws IOException
    {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);

        if (targetVersion < MessagingService.VERSION_20)
            out.writeUTF(String.valueOf(id));
        else
            out.writeInt(id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) timestamp);
        message.serialize(out, targetVersion);
    }

    private static void writeHeader(DataOutput out, int version, boolean compressionEnabled) throws IOException
    {
        // 2 bits: unused.  used to be "serializer type," which was always Binary
        // 1 bit: compression
        // 1 bit: streaming mode
        // 3 bits: unused
        // 8 bits: version
        // 15 bits: unused
        int header = 0;
        if (compressionEnabled)
            header |= 4;
        header |= (version << 8);
        out.writeInt(header);
    }

    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                if (logger.isTraceEnabled())
                    logger.trace("exception closing connection to " + poolReference.endPoint(), e);
            }
            out = null;
            socket = null;
        }
    }

    private boolean connect()
    {
        if (logger.isDebugEnabled())
            logger.debug("attempting to connect to {}", poolReference.endPoint());

        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        while (System.nanoTime() - start < timeout)
        {
            targetVersion = MessagingService.instance().getVersion(poolReference.endPoint());
            try
            {
                socket = poolReference.newSocket();
                socket.setKeepAlive(true);
                if (isLocalDC(poolReference.endPoint()))
                {
                    socket.setTcpNoDelay(INTRADC_TCP_NODELAY);
                }
                else
                {
                    socket.setTcpNoDelay(DatabaseDescriptor.getInterDCTcpNoDelay());
                }
                if (DatabaseDescriptor.getInternodeSendBufferSize() != null)
                {
                    try
                    {
                        socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize());
                    }
                    catch (SocketException se)
                    {
                        logger.warn("Failed to set send buffer size on internode socket.", se);
                    }
                }
                out = new DataOutputStreamPlus(new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE));

                out.writeInt(MessagingService.PROTOCOL_MAGIC);
                writeHeader(out, targetVersion, shouldCompressConnection());
                out.flush();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                int maxTargetVersion = handshakeVersion(in);
                if (maxTargetVersion == NO_VERSION)
                {
                    // no version is returned, so disconnect an try again: we will either get
                    // a different target version (targetVersion < MessagingService.VERSION_12)
                    // or if the same version the handshake will finally succeed
                    logger.debug("Target max version is {}; no version information yet, will retry", maxTargetVersion);
                    disconnect();
                    continue;
                }
                else
                {
                    MessagingService.instance().setVersion(poolReference.endPoint(), maxTargetVersion);
                }

                if (targetVersion > maxTargetVersion)
                {
                    logger.debug("Target max version is {}; will reconnect with that version", maxTargetVersion);
                    disconnect();
                    return false;
                }

                if (targetVersion < maxTargetVersion && targetVersion < MessagingService.current_version)
                {
                    logger.trace("Detected higher max version {} (using {}); will reconnect when queued messages are done",
                                 maxTargetVersion, targetVersion);
                    softCloseSocket();
                }

                out.writeInt(MessagingService.current_version);
                CompactEndpointSerializationHelper.serialize(FBUtilities.getBroadcastAddress(), out);
                if (shouldCompressConnection())
                {
                    out.flush();
                    logger.trace("Upgrading OutputStream to be compressed");
                    if (targetVersion < MessagingService.VERSION_21)
                    {
                        // Snappy is buffered, so no need for extra buffering output stream
                        out = new DataOutputStreamPlus(new SnappyOutputStream(socket.getOutputStream()));
                    }
                    else
                    {
                        // TODO: custom LZ4 OS that supports BB write methods
                        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
                        Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum();
                        out = new DataOutputStreamPlus(new LZ4BlockOutputStream(socket.getOutputStream(),
                                                                            1 << 14,  // 16k block size
                                                                            compressor,
                                                                            checksum,
                                                                            true)); // no async flushing
                    }
                }

                return true;
            }
            catch (IOException e)
            {
                socket = null;
                if (logger.isTraceEnabled())
                    logger.trace("unable to connect to " + poolReference.endPoint(), e);
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
        }
        return false;
    }

    private int handshakeVersion(final DataInputStream inputStream)
    {
        final AtomicInteger version = new AtomicInteger(NO_VERSION);
        final CountDownLatch versionLatch = new CountDownLatch(1);
        new Thread("HANDSHAKE-" + poolReference.endPoint())
        {
            @Override
            public void run()
            {
                try
                {
                    logger.info("Handshaking version with {}", poolReference.endPoint());
                    version.set(inputStream.readInt());
                }
                catch (IOException ex)
                {
                    final String msg = "Cannot handshake version with " + poolReference.endPoint();
                    if (logger.isTraceEnabled())
                        logger.trace(msg, ex);
                    else
                        logger.info(msg);
                }
                finally
                {
                    //unblock the waiting thread on either success or fail
                    versionLatch.countDown();
                }
            }
        }.start();

        try
        {
            versionLatch.await(WAIT_FOR_VERSION_MAX_TIME, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
        return version.get();
    }

    private void expireMessages()
    {
        Iterator<QueuedMessage> iter = backlog.iterator();
        while (iter.hasNext())
        {
            QueuedMessage qm = iter.next();
            if (qm.timestampNanos >= System.nanoTime() - qm.message.getTimeout())
                return;
            iter.remove();
            dropped.incrementAndGet();
        }
    }

    /** messages that have not been retried yet */
    private static class QueuedMessage
    {
        final MessageOut<?> message;
        final int id;
        final long timestampNanos;
        final boolean droppable;

        QueuedMessage(MessageOut<?> message, int id)
        {
            this.message = message;
            this.id = id;
            this.timestampNanos = System.nanoTime();
            this.droppable = MessagingService.DROPPABLE_VERBS.contains(message.verb);
        }

        /** don't drop a non-droppable message just because it's timestamp is expired */
        boolean isTimedOut(long maxTimeNanos, long nowNanos)
        {
            return droppable && timestampNanos < nowNanos - maxTimeNanos;
        }

        boolean shouldRetry()
        {
            return !droppable;
        }
    }

    private static class RetriedQueuedMessage extends QueuedMessage
    {
        RetriedQueuedMessage(QueuedMessage msg)
        {
            super(msg.message, msg.id);
        }

        boolean shouldRetry()
        {
            return false;
        }
    }
}
