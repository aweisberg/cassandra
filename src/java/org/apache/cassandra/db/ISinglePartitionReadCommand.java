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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.TrackedRead.DataRequest;
import org.apache.cassandra.service.reads.tracked.TrackedRead.SummaryRequest;

import static org.apache.cassandra.db.ISinglePartitionReadCommand.Kind.UNTRACKED;

public interface ISinglePartitionReadCommand
{
    enum Kind
    {
        UNTRACKED,
        TRACKED_DATA_READ,
        TRACKED_SUMMARY_READ;

        public boolean isTracked()
        {
            return this != UNTRACKED;
        }

        public static final IVersionedSerializer<Kind> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Kind kind, DataOutputPlus out, int version) throws IOException
            {
                switch (kind)
                {
                    case UNTRACKED:
                        out.writeByte(0);
                        break;
                    case TRACKED_DATA_READ:
                        out.writeByte(1);
                        break;
                    case TRACKED_SUMMARY_READ:
                        out.writeByte(2);
                        break;
                    default:
                        throw new IllegalStateException("Unhandled kind: " + kind);
                }
            }

            @Override
            public Kind deserialize(DataInputPlus in, int version) throws IOException
            {
                int tag = in.readByte();
                switch (tag)
                {
                    case 0:
                        return UNTRACKED;
                    case 1:
                        return TRACKED_DATA_READ;
                    case 2:
                        return TRACKED_SUMMARY_READ;
                    default:
                        throw new IllegalStateException("Unhandled kind value: " + tag);
                }
            }

            @Override
            public long serializedSize(Kind t, int version)
            {
                return TypeSizes.BYTE_SIZE;
            }
        };
    }

    Kind kind();

    default boolean isTracked()
    {
        return kind().isTracked();
    }

    TableMetadata metadata();

    DecoratedKey partitionKey();

    IVersionedSerializer<ISinglePartitionReadCommand> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ISinglePartitionReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            if (version >= MessagingService.VERSION_52)
                Kind.serializer.serialize(command.kind(), out, version);
            else
                Preconditions.checkArgument(command.kind() == UNTRACKED);

            switch (command.kind())
            {
                case UNTRACKED:
                    ReadCommand.serializer.serialize((ReadCommand) command, out, version);
                    break;
                case TRACKED_DATA_READ:
                    DataRequest.serializer.serialize((DataRequest) command, out, version);
                    break;
                case TRACKED_SUMMARY_READ:
                    SummaryRequest.serializer.serialize((SummaryRequest) command, out, version);
                    break;
                default:
                    throw new IllegalStateException("Unhandled kind: " + command.kind());
            }
        }

        @Override
        public ISinglePartitionReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {

            Kind kind = version >= MessagingService.VERSION_52 ? ISinglePartitionReadCommand.Kind.serializer.deserialize(in, version) : UNTRACKED;
            switch (kind)
            {
                case UNTRACKED:
                    return (SinglePartitionReadCommand)ReadCommand.serializer.deserialize(in, version);
                case TRACKED_DATA_READ:
                    return DataRequest.serializer.deserialize(in, version);
                case TRACKED_SUMMARY_READ:
                    return SummaryRequest.serializer.deserialize(in, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + kind);
            }
        }

        @Override
        public long serializedSize(ISinglePartitionReadCommand command, int version)
        {
            long size = 0;
            if (version >= MessagingService.VERSION_52)
                size += Kind.serializer.serializedSize(command.kind(), version);
            else
                Preconditions.checkArgument(command.kind() == UNTRACKED);

            switch (command.kind())
            {
                case UNTRACKED:
                    return size + ReadCommand.serializer.serializedSize((ReadCommand) command, version);
                case TRACKED_DATA_READ:
                    return size + DataRequest.serializer.serializedSize((DataRequest) command, version);
                case TRACKED_SUMMARY_READ:
                    return size + SummaryRequest.serializer.serializedSize((SummaryRequest) command, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + command.kind());
            }
        }
    };
}
