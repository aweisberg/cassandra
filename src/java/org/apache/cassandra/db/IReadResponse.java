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

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.tracked.TrackedDataResponse;
import org.apache.cassandra.service.reads.tracked.TrackedSummaryResponse;

import static org.apache.cassandra.db.IReadResponse.Kind.UNTRACKED;

public interface IReadResponse
{
    /**
     * Returned as a response for Paxos for summary reads so Paxos knows a read response was provided
     */
    IReadResponse TRACKED_DUMMY = new IReadResponse()
    {
        @Override
        public Kind kind()
        {
            return Kind.TRACKED_DUMMY;
        }
    };

    enum Kind
    {
        UNTRACKED(0),
        TRACKED_DATA(1),
        TRACKED_SUMMARY(2),
        TRACKED_DUMMY(3);

        public final int id;

        Kind(int id)
        {
            this.id = id;
        }

        public boolean isTracked()
        {
            return this != UNTRACKED;
        }

        public static final IVersionedSerializer<Kind> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Kind kind, DataOutputPlus out, int version) throws IOException
            {
                out.write(kind.id);
            }

            @Override
            public Kind deserialize(DataInputPlus in, int version) throws IOException
            {
                int id = in.readByte();
                return Kind.values()[id];
            }

            @Override
            public long serializedSize(Kind t, int version)
            {
                return TypeSizes.BYTE_SIZE;
            }
        };
    }

    Kind kind();

    IVersionedSerializer<IReadResponse> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(IReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            if (version >= MessagingService.VERSION_52)
                Kind.serializer.serialize(response.kind(), out, version);
            else
                Preconditions.checkArgument(response.kind() == UNTRACKED);

            switch (response.kind())
            {
                case UNTRACKED:
                    ReadResponse.serializer.serialize((ReadResponse) response, out, version);
                    break;
                case TRACKED_DATA:
                    TrackedDataResponse.serializer.serialize((TrackedDataResponse) response, out, version);
                    break;
                case TRACKED_SUMMARY:
                    TrackedSummaryResponse.serializer.serialize((TrackedSummaryResponse) response, out, version);
                    break;
                case TRACKED_DUMMY:
                    break;
                default:
                    throw new IllegalStateException("Unhandled kind: " + response.kind());
            }
        }

        @Override
        public IReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {

            Kind kind = version >= MessagingService.VERSION_52 ? Kind.serializer.deserialize(in, version) : UNTRACKED;
            switch (kind)
            {
                case UNTRACKED:
                    return ReadResponse.serializer.deserialize(in, version);
                case TRACKED_DATA:
                    return TrackedDataResponse.serializer.deserialize(in, version);
                case TRACKED_SUMMARY:
                    return TrackedSummaryResponse.serializer.deserialize(in, version);
                case TRACKED_DUMMY:
                    return TRACKED_DUMMY;
                default:
                    throw new IllegalStateException("Unhandled kind: " + kind);
            }
        }

        @Override
        public long serializedSize(IReadResponse response, int version)
        {
            long size = 0;
            if (version >= MessagingService.VERSION_52)
                size += Kind.serializer.serializedSize(response.kind(), version);
            else
                Preconditions.checkArgument(response.kind() == UNTRACKED);

            switch (response.kind())
            {
                case UNTRACKED:
                    return size + ReadResponse.serializer.serializedSize((ReadResponse) response, version);
                case TRACKED_DATA:
                    return size + TrackedDataResponse.serializer.serializedSize((TrackedDataResponse) response, version);
                case TRACKED_SUMMARY:
                    return size + TrackedSummaryResponse.serializer.serializedSize((TrackedSummaryResponse) response, version);
                case TRACKED_DUMMY:
                    return size;
                default:
                    throw new IllegalStateException("Unhandled kind: " + response.kind());
            }
        }
    };
}
