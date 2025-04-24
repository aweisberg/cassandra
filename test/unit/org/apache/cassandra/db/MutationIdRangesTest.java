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

package org.apache.cassandra.db;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationId;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class MutationIdRangesTest
{
    private static final Gen<Long> LOG_ID_GEN = rs -> {
        int hostId = rs.nextInt(1, 4);
        int hostLogId = rs.nextInt(1, 11);
        return CoordinatorLogId.asLong(hostId, hostLogId);
    };

    private static final Gen<Long> SEQUENCE_ID_GEN = rs -> {
        int offset = rs.nextBiasedInt(1, 10_000, 1_000_000);
        return MutationId.sequenceId(offset, offset);
    };

    private static final Gen<MutationId> MUTATION_ID_GEN = rs -> new MutationId(LOG_ID_GEN.next(rs), SEQUENCE_ID_GEN.next(rs));

    private static final Gen<MutationIdRanges> MUTATION_ID_RANGES_GEN = rs -> {
        MutationIdRanges ranges = MutationIdRanges.NONE;
        int numIds = rs.nextBiasedInt(0, 10, 1000);
        for (int i = 0; i < numIds; i++)
            ranges = ranges.add(MUTATION_ID_GEN.next(rs));
        return ranges;
    };

    @Test
    public void roundtripSerde()
    {
        qt()
        .forAll(MUTATION_ID_RANGES_GEN)
        .check(ranges -> {
            try (DataOutputBuffer outputBuffer = DataOutputBuffer.scratchBuffer.get())
            {
                MutationIdRanges.serializer.serialize(ranges, outputBuffer, MessagingService.current_version);
                byte[] bytes = outputBuffer.toByteArray();
                try (DataInputBuffer inputBuffer = new DataInputBuffer(bytes))
                {
                    MutationIdRanges deserialized = MutationIdRanges.serializer.deserialize(inputBuffer, MessagingService.current_version);
                    Assertions.assertThat(ranges).isEqualTo(deserialized);
                    Assertions.assertThat(bytes.length).isEqualTo(MutationIdRanges.serializer.serializedSize(ranges, MessagingService.current_version));
                }
            }
        });
    }

    @Test
    public void monotonicAdd()
    {
        qt()
        .forAll(Gens.lists(MUTATION_ID_GEN).ofSizeBetween(3, 100))
        .check(ids -> {
            MutationIdRanges ranges = MutationIdRanges.NONE;
            for (MutationId id : ids)
            {
                MutationIdRanges updated = ranges.add(id);
                int originalOffset = ranges.maxOffset(id.logId());
                int updatedOffset = updated.maxOffset(id.logId());
                Assertions.assertThat(updatedOffset).isGreaterThanOrEqualTo(originalOffset);
                Assertions.assertThat(updatedOffset).isEqualTo(Math.max(originalOffset, id.offset()));

                ranges = updated;
            }
        });
    }

    @Test
    public void monotonicMerge()
    {
        qt()
        .forAll(MUTATION_ID_RANGES_GEN, MUTATION_ID_RANGES_GEN)
        .check((left, right) -> {
            MutationIdRanges merged = left.merge(right);
            for (Long logId : merged.ids.keySet())
            {
                int leftOffset = left.maxOffset(logId);
                int rightOffset = right.maxOffset(logId);
                int mergedOffset = merged.maxOffset(logId);
                Assertions.assertThat(mergedOffset).isGreaterThanOrEqualTo(leftOffset);
                Assertions.assertThat(mergedOffset).isGreaterThanOrEqualTo(rightOffset);
                Assertions.assertThat(mergedOffset).isIn(leftOffset, rightOffset);
            }
        });
    }
}