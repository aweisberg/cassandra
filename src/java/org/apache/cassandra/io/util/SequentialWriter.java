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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.utils.Throwables.merge;

import org.apache.cassandra.utils.SyncUtil;

/**
 * Adds buffering, mark, and fsyncing to OutputStream.  We always fsync on close; we may also
 * fsync incrementally if Config.trickle_fsync is enabled.
 */
public class SequentialWriter extends BufferedDataOutputStreamPlus implements Transactional
{
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    // absolute path to the given file
    private final String filePath;

    private int directoryFD;
    // directory should be synced only after first file sync, in other words, only once per file
    private boolean directorySynced = false;

    // Offset for start of buffer relative to underlying file
    protected long bufferOffset;

    protected final FileChannel fchannel;

    // whether to do trickling fsync() to avoid sudden bursts of dirty buffer flushing by kernel causing read
    // latency spikes
    private boolean trickleFsync;
    private int trickleFsyncByteInterval;
    private int bytesSinceTrickleFsync = 0;

    protected long lastFlushOffset;

    protected Runnable runPostFlush;

    private final TransactionalProxy txnProxy = txnProxy();
    protected Descriptor descriptor;

    //If buffer is not full, stage the bytes for compression/checksumming because the format doesn't handle
    //non-uniform block sizes
    private ByteBuffer stagingBuffer;

    // due to lack of multiple-inheritance, we proxy our transactional implementation
    protected class TransactionalProxy extends AbstractTransactional
    {
        private boolean deleteFile = true;

        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            if (directoryFD >= 0)
            {
                try { CLibrary.tryCloseFD(directoryFD); }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                directoryFD = -1;
            }

            // close is idempotent
            try { channel.close(); }
            catch (Throwable t) { accumulate = merge(accumulate, t); }

            if (buffer != null)
            {
                try { FileUtils.clean(buffer); }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                buffer = null;
            }

            if (stagingBuffer != null)
            {
                try { FileUtils.clean(stagingBuffer); }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                stagingBuffer = null;
            }

            return accumulate;
        }

        protected void doPrepare()
        {
            syncInternal();
            // we must cleanup our file handles during prepareCommit for Windows compatibility as we cannot rename an open file;
            // TODO: once we stop file renaming, remove this for clarity
            releaseFileHandle();
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            if (deleteFile)
                return FileUtils.deleteWithConfirm(filePath, false, accumulate);
            else
                return accumulate;
        }
    }

    private static FileChannel openChannel(File file) {
        try
        {
            if (file.exists())
                return FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            else
                return FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public SequentialWriter(File file, int bufferSize, BufferType bufferType, boolean requiresBlockAlignedFlushing)
    {
        super(openChannel(file), bufferType.allocate(bufferSize));
        allowDirectWritesToChannel = false;
        fchannel = (FileChannel)channel;

        filePath = file.getAbsolutePath();

        // Allow children to allocate buffer as direct (snappy compression) if necessary
        buffer = bufferType.allocate(bufferSize);

        this.trickleFsync = DatabaseDescriptor.getTrickleFsync();
        this.trickleFsyncByteInterval = DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024;

        directoryFD = CLibrary.tryOpenDirectory(file.getParent());
        stagingBuffer = requiresBlockAlignedFlushing ? bufferType.allocate(buffer.capacity()) : null;
    }

    /**
     * Open a heap-based, non-compressed SequentialWriter
     */
    public static SequentialWriter open(File file)
    {
        return new SequentialWriter(file, DEFAULT_BUFFER_SIZE, BufferType.ON_HEAP, false);
    }

    public static ChecksummedSequentialWriter open(File file, File crcPath)
    {
        return new ChecksummedSequentialWriter(file, DEFAULT_BUFFER_SIZE, crcPath);
    }

    public static CompressedSequentialWriter open(String dataFilePath,
                                                  String offsetsPath,
                                                  CompressionParams parameters,
                                                  MetadataCollector sstableMetadataCollector)
    {
        return new CompressedSequentialWriter(new File(dataFilePath), offsetsPath, parameters, sstableMetadataCollector);
    }

    /**
     * Synchronize file contents with disk.
     */
    public void sync()
    {
        syncInternal();
    }

    protected void syncDataOnlyInternal()
    {
        try
        {
            SyncUtil.force(fchannel, false);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    /*
     * This is only safe to call before truncation or close for CompressedSequentialWriter
     * Otherwise it will leave a non-uniform size compressed block in the middle of the file
     * and the compressed format can't handle that.
     */
    protected void syncInternal()
    {
        doFlush(true);
        syncDataOnlyInternal();

        if (!directorySynced)
        {
            SyncUtil.trySync(directoryFD);
            directorySynced = true;
        }
    }

    @Override
    protected void doFlush()
    {
        doFlush(false);
    }

    private void doFlush(boolean definitelyFlush)
    {
        stageAndMaybeFlush(definitelyFlush);

        if (trickleFsync)
        {
            bytesSinceTrickleFsync += buffer.position();
            if (bytesSinceTrickleFsync >= trickleFsyncByteInterval)
            {
                syncDataOnlyInternal();
                bytesSinceTrickleFsync = 0;
            }
        }

        // Remember that we wrote, so we don't write it again on next flush().
        resetBuffer();
    }

    public void setPostFlushListener(Runnable runPostFlush)
    {
        assert this.runPostFlush == null;
        this.runPostFlush = runPostFlush;
    }

    /*
     * When flushing data as part of resetAndTruncateMark it is ok to flush the partial bytes
     * and then let truncation do its thing hence the definitelyFlush parameter. Also definitely
     * need to flush when closing.
     */
    protected void stageAndMaybeFlush(boolean definitelyFlush)
    {
        //No block aligned flushing was requested
        if (stagingBuffer == null)
            flushData(buffer);

        //Do we need to copy the bytes in order to create completely full blocks
        //for the compressed format?
        if (!buffer.hasRemaining() && stagingBuffer.position() == 0 && !definitelyFlush)
        {
            //No it's already full and no bytes are staged
            flushData(buffer);
        }
        else
        {
            //Need to arrange perfectly aligned blocks for compression/checksumming, bytes may be staged already
            buffer.flip();
            while (buffer.hasRemaining())
            {
                if (stagingBuffer.hasRemaining())
                {
                    int originalLimit = buffer.limit();
                    int toCopy = Math.min(stagingBuffer.remaining(), buffer.remaining());
                    buffer.limit(buffer.position() + toCopy);
                    stagingBuffer.put(buffer);
                    buffer.limit(originalLimit);
                }
                else
                {
                    flushData(stagingBuffer);
                    stagingBuffer.clear();
                }
            }

            //Could exit the loop with the staging buffer full because buffer is empty
            //and filled it exactly.
            //Alternatively if we are definitely supposed to flush and there are bytes to flush, do it
            if (!stagingBuffer.hasRemaining() || (definitelyFlush && stagingBuffer.position() > 0))
            {
                flushData(stagingBuffer);
                stagingBuffer.clear();
            }
        }
    }

    /**
     * Override this method instead of overriding flush()
     * @throws FSWriteError on any I/O error.
     */
    protected void flushData(ByteBuffer buffer)
    {
        try
        {
            buffer.flip();
            channel.write(buffer);
            lastFlushOffset += buffer.position();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
        if (runPostFlush != null)
            runPostFlush.run();
    }

    public long getFilePointer()
    {
        return current();
    }

    /**
     * Returns the current file pointer of the underlying on-disk file.
     * Note that since write works by buffering data, the value of this will increase by buffer
     * size and not every write to the writer will modify this value.
     * Furthermore, for compressed files, this value refers to compressed data, while the
     * writer getFilePointer() refers to uncompressedFile
     *
     * @return the current file pointer
     */
    public long getOnDiskFilePointer()
    {
        return getFilePointer();
    }

    public long length()
    {
        try
        {
            return Math.max(current(), fchannel.size());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    public String getPath()
    {
        return filePath;
    }

    protected void resetBuffer()
    {
        bufferOffset = current();
        buffer.clear();
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public FileMark mark()
    {
        return new BufferedFileWriterMark(current());
    }

    /**
     * Drops all buffered data that's past the limits of our new file mark + buffer capacity, or syncs and truncates
     * the underlying file to the marked position
     */
    public void resetAndTruncate(FileMark mark)
    {
        assert mark instanceof BufferedFileWriterMark;

        long previous = current();
        long truncateTarget = ((BufferedFileWriterMark) mark).pointer;

        // If we're resetting to a point within our buffered data, just adjust our buffered position to drop bytes to
        // the right of the desired mark.
        if (previous - truncateTarget <= buffer.position())
        {
            buffer.position(buffer.position() - ((int) (previous - truncateTarget)));
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        syncInternal();

        // truncate file to given position
        truncate(truncateTarget);

        try
        {
            fchannel.position(truncateTarget);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }

        resetBuffer();
    }

    public long getLastFlushOffset()
    {
        return lastFlushOffset;
    }

    public void truncate(long toSize)
    {
        try
        {
            fchannel.truncate(toSize);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public boolean isOpen()
    {
        return channel.isOpen();
    }

    public SequentialWriter setDescriptor(Descriptor descriptor)
    {
        this.descriptor = descriptor;
        return this;
    }

    public final void prepareToCommit()
    {
        txnProxy.prepareToCommit();
    }

    public final Throwable commit(Throwable accumulate)
    {
        return txnProxy.commit(accumulate);
    }

    public final Throwable abort(Throwable accumulate)
    {
        return txnProxy.abort(accumulate);
    }

    @Override
    public final void close()
    {
        txnProxy.close();
    }

    public final void finish()
    {
        txnProxy.finish();
    }

    protected TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    public void deleteFile(boolean val)
    {
        txnProxy.deleteFile = val;
    }

    public void releaseFileHandle()
    {
        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, filePath);
        }
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedFileWriterMark implements FileMark
    {
        final long pointer;

        public BufferedFileWriterMark(long pointer)
        {
            this.pointer = pointer;
        }
    }
}
