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

import java.util.Collection;

import org.apache.cassandra.locator.InetAddressAndPorts;

/**
 * Contains forward to information until it can be serialized as part of a message using a version
 * specific serialization
 */
public class ForwardToContainer
{
    public final Collection<InetAddressAndPorts> targets;
    public final int[] messageIds;

    public ForwardToContainer(Collection<InetAddressAndPorts> targets,
                              int[] messageIds)
    {
        this.targets = targets;
        this.messageIds = messageIds;
    }
}
