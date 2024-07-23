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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class TxnRangeReadResult implements TxnResult
{
    public final PartitionIterator partitions;

    public TxnRangeReadResult(PartitionIterator partitions)
    {
        this.partitions = partitions;
    }

    @Override
    public Kind kind()
    {
        return Kind.range_read;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        return 0;
    }

    // Should never need to serialize the result since it is computed at the coordinator and then processed outside
    // Accord
    public static final TxnResultSerializer<TxnRangeReadResult> serializer = new TxnResultSerializer<TxnRangeReadResult>()
    {
        @Override
        public void serialize(TxnRangeReadResult data, DataOutputPlus out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TxnRangeReadResult deserialize(DataInputPlus in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long serializedSize(TxnRangeReadResult data, int version)
        {
            throw new UnsupportedOperationException();
        }
    };
}
