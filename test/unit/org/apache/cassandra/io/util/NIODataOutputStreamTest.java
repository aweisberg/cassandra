package org.apache.cassandra.io.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.*;

public class NIODataOutputStreamTest
{
    WritableByteChannel adapter = new WritableByteChannel()
    {

        @Override
        public boolean isOpen()  {return true;}

        @Override
        public void close() throws IOException {}

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            int retval = src.remaining();
            while (src.hasRemaining())
                generated.write(src.get());
            return retval;
        }

    };

    NIODataOutputStreamPlus fakeStream = new NIODataOutputStreamPlus(adapter, 8);

    @SuppressWarnings("resource")
    @Test(expected = NullPointerException.class)
    public void testNullChannel()
    {
        new NIODataOutputStreamPlus(null, 8);
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalArgumentException.class)
    public void testTooSmallBuffer()
    {
        new NIODataOutputStreamPlus(adapter, 7);
    }

    @Test(expected = NullPointerException.class)
    public void testNullBuffer() throws Exception
    {
        byte type[] = null;
        fakeStream.write(type, 0, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeOffset() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, -1, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeLength() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testTooBigLength() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 0, 11);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testTooBigLengthWithOffset() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 8, 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testTooBigOffset() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 11, 1);
    }

    private Random r;
    private ByteArrayOutputStream generated;
    private NIODataOutputStreamPlus ndosp;

    private ByteArrayOutputStream canonical;
    private DataOutputStreamPlus dosp;

    void setUp()
    {
        long seed = System.nanoTime();
        r = new Random(seed);
        generated = new ByteArrayOutputStream();
        canonical = new ByteArrayOutputStream();
        dosp = new DataOutputStreamPlus(canonical);
        ndosp = new NIODataOutputStreamPlus(adapter, 4096);
    }

    @Test
    public void testFuzz() throws Exception
    {
        for (int ii = 0; ii < 30; ii++)
            fuzzOnce();
    }

    String simple = "foobar42";
    String twoByte = "ƀ";
    String threeByte = "㒨";
    String fourByte = "𠝹";

    private void fuzzOnce() throws Exception
    {
        setUp();
        while (generated.size() < 1024 * 1024 * 8)
        {
            int action = r.nextInt(18);

            switch (action)
            {
            case 0: {
                generated.flush();
                dosp.flush();
                break;
            }
            case 1: {
                int val = r.nextInt();
                dosp.write(val);
                ndosp.write(val);
                break;
            }
            case 2: {
                byte randomBytes[] = new byte[r.nextInt(4096 * 2 + 1)];
                r.nextBytes(randomBytes);
                dosp.write(randomBytes);
                ndosp.write(randomBytes);
                break;
            }
            case 3: {
                byte randomBytes[] = new byte[r.nextInt(4096 * 2 + 1)];
                r.nextBytes(randomBytes);
                int offset = randomBytes.length == 0 ? 0 : r.nextInt(randomBytes.length);
                int length = randomBytes.length == 0 ? 0 : r.nextInt(randomBytes.length - offset);
                dosp.write(randomBytes, offset, length);
                ndosp.write(randomBytes, offset, length);
                break;
            }
            case 4: {
                boolean val = r.nextInt(2) == 0;
                dosp.writeBoolean(val);
                ndosp.writeBoolean(val);
                break;
            }
            case 5: {
                int val = r.nextInt();
                dosp.writeByte(val);
                ndosp.writeByte(val);
                break;
            }
            case 6: {
                int val = r.nextInt();
                dosp.writeShort(r.nextInt());
                ndosp.writeShort(val);
                break;
            }
            case 7: {
                int val = r.nextInt();
                dosp.writeChar(val);
                ndosp.writeChar(val);
                break;
            }
            case 8: {
                int val = r.nextInt();
                dosp.writeInt(val);
                ndosp.writeInt(val);
                break;
            }
            case 9: {
                int val = r.nextInt();
                dosp.writeLong(val);
                ndosp.writeLong(val);
                break;
            }
            case 10: {
                float val = r.nextFloat();
                dosp.writeFloat(val);
                ndosp.writeFloat(val);
                break;
            }
            case 11: {
                double val = r.nextDouble();
                dosp.writeDouble(val);
                ndosp.writeDouble(val);
                break;
            }
            case 12: {
                dosp.writeBytes(simple);
                ndosp.writeBytes(simple);
                break;
            }
            case 13: {
                dosp.writeChars(twoByte);
                ndosp.writeChars(twoByte);
                break;
            }
            case 14: {
                String str = simple + twoByte+ threeByte + fourByte;
                dosp.writeUTF(str);
                ndosp.writeUTF(str);
                break;
            }
            case 15: {
                ByteBuffer buf = ByteBuffer.allocate(r.nextInt(1024 * 8 + 1));
                r.nextBytes(buf.array());
                buf.position(buf.capacity() == 0 ? 0 : r.nextInt(buf.capacity()));
                buf.limit(buf.position() + (buf.capacity() - buf.position() == 0 ? 0 : r.nextInt(buf.capacity() - buf.position())));
                ndosp.write(buf.duplicate());
                dosp.write(buf.duplicate());
                break;
            }
            case 16: {
                ByteBuffer buf = ByteBuffer.allocateDirect(r.nextInt(1024 * 8 + 1));
                while (buf.hasRemaining())
                    buf.put((byte)r.nextInt());
                buf.position(buf.capacity() == 0 ? 0 : r.nextInt(buf.capacity()));
                buf.limit(buf.position() + (buf.capacity() - buf.position() == 0 ? 0 : r.nextInt(buf.capacity() - buf.position())));
                ndosp.write(buf.duplicate());
                dosp.write(buf.duplicate());
                break;
            }
            case 17: {
                Memory buf = Memory.allocate(r.nextInt(1024 * 8 - 1) + 1);
                for (int ii = 0; ii < buf.size(); ii++)
                    buf.setByte(ii, (byte)r.nextInt());
                long offset = buf.size() == 0 ? 0 : r.nextInt((int)buf.size());
                long length = offset + (buf.size() - offset == 0 ? 0 : r.nextInt((int)(buf.size() - offset)));
                ndosp.write(buf, offset, length);
                dosp.write(buf, offset, length);
                break;            }
            default:
                fail("Shouldn't reach here");
            }
        }

        ndosp.flush();
        dosp.flush();

        Arrays.equals(generated.toByteArray(), canonical.toByteArray());
    }
}
