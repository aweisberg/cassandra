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

import io.netty.util.internal.PlatformDependent;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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
import org.apache.cassandra.utils.concurrent.MpscLinkedQueueBase;
import org.apache.cassandra.utils.concurrent.MpscLinkedQueueNode;
import org.xerial.snappy.SnappyOutputStream;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import sun.misc.Contended;

import com.google.common.util.concurrent.Uninterruptibles;

public class OutboundTcpConnection extends MpscLinkedQueueBase<QueuedMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final int TRUE = 1;
    private static final int FALSE = 0;

    private static final Executor executor = StageManager.getStage(Stage.NETWORK_WRITE);

    private static final AtomicReferenceFieldUpdater<OutboundTcpConnection, MpscLinkedQueueNode<QueuedMessage>> headRefUpdater =
            PlatformDependent.newAtomicReferenceFieldUpdater(OutboundTcpConnection.class, "headRef");

    private static final AtomicReferenceFieldUpdater<OutboundTcpConnection, MpscLinkedQueueNode<QueuedMessage>> tailRefUpdater =
            PlatformDependent.newAtomicReferenceFieldUpdater(OutboundTcpConnection.class, "tailRef");

    private static final AtomicIntegerFieldUpdater<OutboundTcpConnection> needsWakeupUpdater =
            PlatformDependent.newAtomicIntegerFieldUpdater(OutboundTcpConnection.class, "needsWakeup");

    private static final AtomicLongFieldUpdater<OutboundTcpConnection> droppedUpdater  =
            PlatformDependent.newAtomicLongFieldUpdater(OutboundTcpConnection.class, "dropped");

    private static final AtomicLongFieldUpdater<OutboundTcpConnection> completedUpdater  =
            PlatformDependent.newAtomicLongFieldUpdater(OutboundTcpConnection.class, "completed");

    @SuppressWarnings("rawtypes")
    private static final MessageOut CLOSE_SENTINEL = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);

    private volatile boolean isStopped = false;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    public static final int WAIT_FOR_VERSION_MAX_TIME = 5000;
    private static final int NO_VERSION = Integer.MIN_VALUE;

    static final int LZ4_HASH_SEED = 0x9747b28c;

    private final OutboundTcpConnectionPool poolReference;

    private DataOutputStreamPlus out;
    private Socket socket;
    @Contended("Dispatcher")
    private volatile long completed;
    @Contended("Dispatcher")
    private volatile MpscLinkedQueueNode<QueuedMessage> headRef;

    private int targetVersion;

    private final String threadName;

    @Contended("Producer")
    private volatile long dropped = 0;
    @Contended("Producer")
    private volatile int needsWakeup = TRUE;
    @Contended("Producer")
    private volatile MpscLinkedQueueNode<QueuedMessage> tailRef;

    public OutboundTcpConnection(OutboundTcpConnectionPool pool)
    {
        super();
        threadName = "WRITE-" + pool.endPoint();
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
//        if (backlog.size() > 1024)
//            expireMessages();
        offer(new QueuedMessage(message, id));

        if (needsWakeup == TRUE && needsWakeupUpdater.compareAndSet(this, TRUE, FALSE)) {
            executor.execute(dispatchTask);
        }
    }

    void closeSocket(boolean destroyThread)
    {
        clear();
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

    private Runnable dispatchTask = new Runnable()
    {
        @Override
        public void run()
        {
            while (true) {
                dispatchQueue();

                needsWakeup = TRUE;

                if (isEmpty() || !needsWakeupUpdater.compareAndSet(OutboundTcpConnection.this, TRUE, FALSE))
                    return;
            }
        }
    };

    private void dispatchQueue()
    {
        QueuedMessage qm = null;
        while ((qm = poll()) != null) {
            try
            {
                MessageOut<?> m = qm.message;
                if (m == CLOSE_SENTINEL)
                {
                    disconnect();
                    if (isStopped)
                        return;
                    continue;
                }
                if (qm.isTimedOut(m.getTimeout()))
                    droppedUpdater.incrementAndGet(this);
                else if (socket != null || connect())
                    writeConnected(qm, isEmpty());
                else
                    // clear out the queue, else gossip messages back up.
                    clear();
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // really shouldn't get here, as exception handling in writeConnected() is reasonably robust
                // but we want to catch anything bad we don't drop the messages in the current batch
                logger.error("error processing a message intended for {}", poolReference.endPoint(), e);
            }
        }
    }

    public int getPendingMessages()
    {
        return 0;
        //return size();
    }

    public long getCompletedMesssages()
    {
        return completed;
    }

    public long getDroppedMessages()
    {
        return dropped;
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

            writeInternal(qm.message, qm.id, qm.timestamp);

            completedUpdater.lazySet(this, completed + 1);
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
                    offer(new RetriedQueuedMessage(qm));
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
                out = new DataOutputStreamPlus(new BufferedOutputStream(socket.getOutputStream(), 1024 * 32));

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

//    private void expireMessages()
//    {
//        Iterator<QueuedMessage> iter = backlog.iterator();
//        while (iter.hasNext())
//        {
//            QueuedMessage qm = iter.next();
//            if (qm.timestamp >= System.currentTimeMillis() - qm.message.getTimeout())
//                return;
//            iter.remove();
//            droppedUpdater.incrementAndGet(this);
//        }
//    }



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

    protected final MpscLinkedQueueNode<QueuedMessage> tailRef() {
        return tailRef;
    }

    @Override
    protected final void setTailRef(MpscLinkedQueueNode<QueuedMessage> tailRef) {
        this.tailRef = tailRef;
    }

    @SuppressWarnings("unchecked")
    protected final MpscLinkedQueueNode<QueuedMessage> getAndSetTailRef(MpscLinkedQueueNode<QueuedMessage> tailRef) {
        // LOCK XCHG in JDK8, a CAS loop in JDK 7/6
        return (MpscLinkedQueueNode<QueuedMessage>) tailRefUpdater.getAndSet(this, tailRef);
    }

    protected final MpscLinkedQueueNode<QueuedMessage> headRef() {
        return headRef;
    }

    protected final void setHeadRef(MpscLinkedQueueNode<QueuedMessage> headRef) {
        this.headRef = headRef;
    }

    protected final void lazySetHeadRef(MpscLinkedQueueNode<QueuedMessage> headRef) {
        headRefUpdater.lazySet(this, headRef);
    }
}
