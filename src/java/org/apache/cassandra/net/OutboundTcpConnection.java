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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.HdrHistogram.Histogram;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;
import org.xerial.snappy.SnappyOutputStream;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.google.common.util.concurrent.Uninterruptibles;

public class OutboundTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static volatile long TIMESTAMP_BASE[] = new long[] { System.currentTimeMillis(), System.nanoTime() };

    /*
     * System.currentTimeMillis() is 25 nanoseconds. This is 2 nanoseconds (maybe) according to JMH.
     * Faster than calling both currentTimeMillis() and nanoTime() for every outbound message.
     */
    private static final long convertNanoTimeToCurrentTimeMillis(long nanoTime) {
        final long timestampBase[] = TIMESTAMP_BASE;
        return timestampBase[0] + TimeUnit.NANOSECONDS.toMillis(nanoTime - timestampBase[1]);
    }

    public static final long OUT_BATCH_WINDOW = Long.getLong("OUT_BATCH_WINDOW", 100);

    static {
        logger.info("batch window is " + OUT_BATCH_WINDOW);

        //Pick up updates from NTP periodically
        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                TIMESTAMP_BASE = new long[] {System.currentTimeMillis(), System.nanoTime() };
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    public static final Semaphore trace = new Semaphore(1);

    private static final MessageOut CLOSE_SENTINEL = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
    private volatile boolean isStopped = false;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    public static final int WAIT_FOR_VERSION_MAX_TIME = 5000;
    private static final int NO_VERSION = Integer.MIN_VALUE;

    static final int LZ4_HASH_SEED = 0x9747b28c;

    private final BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<>();

    private final OutboundTcpConnectionPool poolReference;

    private DataOutputStreamPlus out;
    private Socket socket;
    private volatile long completed;
    private final AtomicLong dropped = new AtomicLong();
    private volatile int currentMsgBufferCount = 0;
    private int targetVersion;

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

    public interface CoalescingStrategy {
        /*
         * Notify of a sample, but don't attempt to coalesce. Used when coalescing has already been done.
         */
        long notifyOfSample(long sample);

        /*
         * Notify of a sample, and sleep if it makes sense in order to allow coalescing to occur
         *
         * Returns true of a sleep occured and the task queue should be checked again.
         */
        boolean notifyAndSleep(long sample);
    }

    /*
     * Returns the wrong answer for the first N samples, but does it really matter?
     */
    public static class MovingAverageCoalescingStrategy implements CoalescingStrategy {
        private final int samples[] = new int[16];
        private long lastSample = 0;
        private int index = 0;
        private long sum = 0;

        private final long maxCoalesceWindow;

        private long coalesceDecision = -1;

        public MovingAverageCoalescingStrategy(int maxCoalesceWindow) {
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            for (int ii = 0; ii < samples.length; ii++)
                samples[ii] = Integer.MAX_VALUE;
        }

        private long logSample(int value) {
            sum -= samples[index];
            sum += value;
            samples[index] = value;
            index++;
            index = index & ((1 << 4) - 1);
            return sum / 8;
        }

        @Override
        public long notifyOfSample(long sample)
        {
            if (sample <= lastSample) {
                return logSample(1);
            } else {
                final int delta = (int)(Math.min(Integer.MAX_VALUE, sample - lastSample));
                lastSample = sample;
                return logSample(delta);
            }
        }

        @Override
        public boolean notifyAndSleep(long sample)
        {
            long average = notifyOfSample(sample);

            if (average < maxCoalesceWindow) {
                if (coalesceDecision == -1 || average > coalesceDecision) {
                    coalesceDecision = Math.min(maxCoalesceWindow, average * 2);
                }
                long now = System.nanoTime();
                final long timer = now + coalesceDecision;
                do {
                    LockSupport.parkNanos(timer - now);
                } while ((now = System.nanoTime()) < timer);
                return true;
            }
             coalesceDecision = -1;
            return false;
        }
    }

    private final CoalescingStrategy cs = new MovingAverageCoalescingStrategy(Integer.getInteger("MAX_COALESCE_WINDOW", 200));

    private static final boolean DISABLE_COALESCING = Boolean.getBoolean("DISABLE_COALESCING");

    public static void main(String args[]) throws Exception {
        FileInputStream fis = new FileInputStream("/tmp/trace");
        BufferedInputStream bis = new BufferedInputStream(fis, 1024 * 64);
        DataInputStream dis = new DataInputStream(bis);

        Histogram h = new Histogram(Long.MAX_VALUE, 3);
        System.out.println("Footprint " + h.getEstimatedFootprintInBytes());
        long last = 0;
        long samplesFound = 0;
        while (true) {
            try {
                long sample = dis.readLong();
                samplesFound++;
                if (last == 0) {
                    last = sample;
                    continue;
                }
                long delta = sample - last;
                if (delta < 0) delta = 1;
                h.recordValue(delta);
                last = sample;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        System.out.println("Samples found " + samplesFound);
        h.outputPercentileDistribution(System.out, 1000.0);
    }

    public void run()
    {
        DataOutputStream dos = null;
        if (trace.tryAcquire()) {
            FileOutputStream fos;
            try
            {
                fos = new FileOutputStream("/tmp/trace");
                dos = new DataOutputStream(new BufferedOutputStream(fos, 1024 * 64));
            }
            catch (FileNotFoundException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        // keeping list (batch) size small for now; that way we don't have an unbounded array (that we never resize)
        final List<QueuedMessage> drainedMessages = new ArrayList<>(128);

        outer:
        while (true)
        {
            if (backlog.drainTo(drainedMessages, 128) == 0)
            {
                try
                {
                    drainedMessages.add(backlog.take());
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
                if (DISABLE_COALESCING) {
                    backlog.drainTo(drainedMessages, 127);
                }
            }

            int remainingSlots = 128 - drainedMessages.size();
            if (!DISABLE_COALESCING) {
                if (cs.notifyAndSleep(drainedMessages.get(0).timestampNanos)) {
                    backlog.drainTo(drainedMessages, remainingSlots);
                }
            }

            currentMsgBufferCount = drainedMessages.size();

            int count = drainedMessages.size();
            boolean logTimestamp = false;
            for (QueuedMessage qm : drainedMessages)
            {
                if (dos != null) {
                    try
                    {
                        dos.writeLong(qm.timestampNanos);
                    }
                    catch (IOException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
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

                    if (logTimestamp) {
                        cs.notifyOfSample(qm.timestampNanos);
                    }
                    logTimestamp = true;

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
                    socket.setTcpNoDelay(true);
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
                out = new DataOutputStreamPlus(new BufferedOutputStream(socket.getOutputStream(), 1024 * 64));

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
