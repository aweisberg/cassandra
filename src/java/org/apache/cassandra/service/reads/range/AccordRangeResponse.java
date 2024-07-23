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

package org.apache.cassandra.service.reads.range;

import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Future;

public class AccordRangeResponse extends AbstractIterator<RowIterator> implements IRangeResponse
{
    private final Future<ConsensusAttemptResult> resultFuture;
    private PartitionIterator result;

    public AccordRangeResponse(Future<ConsensusAttemptResult> resultFuture)
    {
        this.resultFuture = resultFuture;
    }

    private void waitForResponse()
    {
        if (result != null)
            return;
        try
        {
            // TODO (required): Handle retry on different system
            result = Uninterruptibles.getUninterruptibly(resultFuture).serialReadResult;
        }
        catch (ExecutionException e)
        {
            // Preserve the execution exception as a suppressed exception
            // but throw the actual cause since the type is frequently significant in error handling
            Throwable cause = e.getCause();
            cause.addSuppressed(e);
            Throwables.throwAsUncheckedException(cause);
        }
    }

    @Override
    protected RowIterator computeNext()
    {
        waitForResponse();
        return result.hasNext() ? result.next() : endOfData();
    }

    @Override
    public void close()
    {
        resultFuture.addCallback((result, failure) -> {
            if (result != null && result.serialReadResult != null)
                result.serialReadResult.close();
        });
    }
}
