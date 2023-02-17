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

package org.apache.cassandra.repair;

import java.math.BigInteger;
import java.util.List;

import accord.api.RoutingKey;
import accord.primitives.Ranges;
import org.apache.cassandra.dht.AccordSplitter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ConsensusMigrationStateStore.ConsensusMigrationRepairResult;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.System.nanoTime;
import static java.util.Collections.emptyList;

public class AccordRepairJob extends AbstractRepairJob
{
    private final Ranges ranges;

    private final AccordSplitter splitter;

    private BigInteger rangeStep;

    private Epoch minEpoch = ClusterMetadata.current().epoch;

    public AccordRepairJob(RepairSession repairSession, String cfname)
    {
        super(repairSession, cfname);
        List<Range<Token>> normalizedRanges = Range.normalize(desc.ranges);
        IPartitioner partitioner = normalizedRanges.get(0).left.getPartitioner();
        TokenRange[] tokenRanges = new TokenRange[normalizedRanges.size()];
        for (int i = 0; i < normalizedRanges.size(); i++)
            tokenRanges[i] = new TokenRange(new TokenKey(ks.getName(), normalizedRanges.get(i).left), new TokenKey(ks.getName(), normalizedRanges.get(i).right));
        this.ranges = Ranges.of(tokenRanges);
        this.splitter = partitioner.accordSplitter().apply(Ranges.of(tokenRanges));
    }

    @Override
    protected void runRepair()
    {
        try
        {
            for (accord.primitives.Range range : ranges)
                repairRange((TokenRange)range);
            state.phase.success();
            cfs.metric.repairsCompleted.inc();
            trySuccess(new RepairResult(desc, emptyList(), ConsensusMigrationRepairResult.fromAccordRepair(minEpoch)));
        }
        catch (Throwable t)
        {
            state.phase.fail(t);
            cfs.metric.repairsCompleted.inc();
            tryFailure(t);
        }
    }

    private void repairRange(TokenRange range)
    {
        RoutingKey remainingStart = range.start();
        // TODO validate wrap around is handled correctly, off by one and inclusive/exclusive
        BigInteger rangeSize = splitter.sizeOf(range);
        if (rangeStep == null)
            rangeStep = BigInteger.ONE.max(splitter.divide(rangeSize, 1000));

        BigInteger offset = BigInteger.ZERO;

        TokenRange lastRepaired = null;
        int iteration = 0;
        while (true)
        {
            iteration++;
            if (iteration % 100 == 0)
                rangeStep = rangeStep.multiply(BigInteger.TWO);

            BigInteger remaining = rangeSize.subtract(offset);
            BigInteger length = remaining.min(rangeStep);

            long start = nanoTime();
            try
            {
                // Splitter is approximate so it can't work right up to the end
                TokenRange toRepair;
                if (splitter.compare(offset, rangeSize) >= 0)
                {
                    if (remainingStart.equals(range.end()))
                        return;

                    // Final repair is whatever remains
                    toRepair = range.newRange(remainingStart, range.end());
                }
                else
                {
                    toRepair = splitter.subRange(range, offset, splitter.add(offset, length));
                    checkState(iteration > 1 || toRepair.start().equals(range.start()));
                }
                checkState(!toRepair.equals(lastRepaired), "Shouldn't repair the same range twice");
                checkState(lastRepaired == null || toRepair.start().equals(lastRepaired.end()), "Next range should directly follow previous range");
                lastRepaired = toRepair;
                // TODO Won't work until range transactions work
//                AccordService.instance().barrier(toRepair, minEpoch.getEpoch(), nanoTime(), BarrierType.global_sync, false);
                remainingStart = toRepair.end();
            }
            catch (RuntimeException e)
            {
                // Dependency limit
                cfs.metric.rangeMigrationDependencyLimitFailures.mark();
                throw e;
            }
            catch (Throwable t)
            {
                // unexpected error
                cfs.metric.rangeMigrationUnexpectedFailures.mark();
                throw t;
            }
            finally
            {
                cfs.metric.rangeMigration.addNano(start);
            }

            boolean repairOverflow = false;
            if (repairOverflow)
            {
                offset = offset.subtract(rangeStep);
                if (rangeStep == BigInteger.ONE)
                    throw new IllegalStateException("Unable to repair without overflowing with range step of 1");
                rangeStep = BigInteger.ONE.max(rangeStep.divide(BigInteger.TWO));
                continue;
            }

            offset = offset.add(length);
        }
    }
}
