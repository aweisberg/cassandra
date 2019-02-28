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

package org.apache.cassandra.quicktheories.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.quicktheories.generators.Extensions;
import org.apache.cassandra.quicktheories.generators.FullKey;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

public class InMemoryModel implements ModelState
{
    private final List<Object[]> partitionKeys = new ArrayList<>();
    private final Map<Object[], Partition> partitions = new HashMap<>();

    public void addPrimaryKey(Object[] pk)
    {
        if (partitions.containsKey(pk))
            return;
        this.partitions.put(pk, new Partition(pk));
        this.partitionKeys.add(pk);

    }

    public boolean addFullKey(FullKey fullKey)
    {
        addPrimaryKey(fullKey.partition);
        return partitions.get(fullKey.partition).addFullKey(fullKey);
    }

    public Gen<Object[]> primaryKeyGen()
    {
        return SourceDSL.arbitrary().pick(partitionKeys);
    }

    public Gen<FullKey> fullKeyGen()
    {
        return primaryKeyGen()
               .flatMap(pk -> {
                   List<FullKey> fullKeys = partitions.get(pk).fullKeys;
                   return SourceDSL.arbitrary().pick(fullKeys);
               });
    }

    public List<Object[]> partitionKeys()
    {
        return partitionKeys;
    }

    public Gen<List<FullKey>> fullKeysFromSamePartition(int min, int max)
    {
        return primaryKeyGen()
                .flatMap(pk -> {
                    List<FullKey> fullKeys = partitions.get(pk).fullKeys;
                    return Extensions.subsetGenerator(fullKeys, min, max);
                });
    }

    private static class Partition
    {
        private final Object[] partitionKey;
        private final List<FullKey> fullKeys = new ArrayList<>();
        private final HashSet<FullKey> fullKeysSet = new HashSet<>();

        public Partition(Object[] partitionKey) {
            this.partitionKey = partitionKey;
        }

        public boolean addFullKey(FullKey fullKey){
            if (fullKeysSet.contains(fullKey))
                return false;

            fullKeysSet.add(fullKey);
            fullKeys.add(fullKey);
            return true;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Partition partition = (Partition) o;
            return Arrays.equals(partitionKey, partition.partitionKey) &&
                   Objects.equals(fullKeys, partition.fullKeys) &&
                   Objects.equals(fullKeysSet, partition.fullKeysSet);
        }

        public int hashCode()
        {
            int result = Objects.hash(fullKeys, fullKeysSet);
            result = 31 * result + Arrays.hashCode(partitionKey);
            return result;
        }
    }
}
