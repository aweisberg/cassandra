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

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * When possible use {@link DataOutputStreamAndChannel} instead of this class, as it will
 * be more efficient. This class is only for situations where it cannot be used
 */
public abstract class DataOutputStreamAndChannelPlus extends DataOutputStreamPlus implements DataOutputAndChannelPlus
{
    //Dummy wrapper channel for derived implementations that don't have a channel
    private final WritableByteChannel channel = Channels.newChannel(this);

    //Derived classes and can override and provide a real channel
    protected WritableByteChannel channel()
    {
        return channel;
    }

    @Override
    public WritableByteChannel getChannel() throws IOException
    {
        //Don't allow writes to the underlying channel while data is buffered
        flush();
        return channel();
    }

    @Override
    public void applyToChannel(WBCConsumer c) throws IOException
    {
        //Don't allow writes to the underlying channel while data is buffered
        flush();
        c.apply(channel());
    }
}
