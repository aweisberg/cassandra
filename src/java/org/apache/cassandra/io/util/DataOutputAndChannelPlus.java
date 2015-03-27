package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Interface for DataOutput that exposes a WritableByteChannel potentially allowing for zero copy writes
 * or optimized copies like FileChannel.transferTo
 */
public interface DataOutputAndChannelPlus extends DataOutputPlus
{
    interface WBCFunction
    {
        Object apply(WritableByteChannel c) throws IOException;
    }

    /**
     * Safe way to operate against the underlying channel. Impossible to stash a reference to the channel
     * and forget to flush
     */
    Object applyToChannel(WBCFunction c) throws IOException;
}
