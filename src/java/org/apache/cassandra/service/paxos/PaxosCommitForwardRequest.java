package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Request to forward a Paxos V1 commit operation to a replica coordinator.
 * This is used when the original coordinator is not a replica but needs to
 * execute a Paxos commit for a tracked keyspace that requires MutationId generation.
 */
public class PaxosCommitForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final Commit proposal;
    public final ConsistencyLevel consistencyLevel;

    public PaxosCommitForwardRequest(Commit proposal, ConsistencyLevel consistencyLevel)
    {
        this.proposal = proposal;
        this.consistencyLevel = consistencyLevel;
    }

    public static class Serializer implements IVersionedSerializer<PaxosCommitForwardRequest>
    {
        @Override
        public void serialize(PaxosCommitForwardRequest request, DataOutputPlus out, int version) throws IOException
        {
            Commit.serializer.serialize(request.proposal, out, version);
            out.write(request.consistencyLevel.code);
        }

        @Override
        public PaxosCommitForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Commit proposal = Commit.serializer.deserialize(in, version);
            ConsistencyLevel consistencyLevel = ConsistencyLevel.fromCode(in.readUnsignedByte());
            return new PaxosCommitForwardRequest(proposal, consistencyLevel);
        }

        @Override
        public long serializedSize(PaxosCommitForwardRequest request, int version)
        {
            long size = Commit.serializer.serializedSize(request.proposal, version)
                       + 1; // consistencyLevel.code
            return size;
        }
    }
}