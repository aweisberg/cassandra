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

package org.apache.cassandra.quicktheories.generators;

import java.util.Arrays;

public class FullKey
{
    public final Object[] partition;
    public final Object[] clustering;

    public FullKey(Object[] partition, Object[] clustering)
    {
        this.partition = partition;
        this.clustering = clustering;
    }

    public String toString()
    {
        return "FullKey{" +
               "partition=" + Arrays.toString(partition) +
               ", clustering=" + Arrays.toString(clustering) +
               '}';
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullKey key = (FullKey) o;
        return Arrays.equals(partition, key.partition) &&
               Arrays.equals(clustering, key.clustering);
    }

    public Object[] partitionKey()
    {
        return partition;
    }

    public Object[] clusteringKey()
    {
        return clustering;
    }

    public int hashCode()
    {
        int result = Arrays.hashCode(partition);
        result = 31 * result + Arrays.hashCode(clustering);
        return result;
    }
}
