package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.quicktheories.WithQuickTheories;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IntervalTreeTest implements WithQuickTheories
{
    private static final ISerializer<Integer> INT_SERIALIZER = new ISerializer<>()
    {
        public void serialize(Integer i, DataOutputPlus out) throws IOException
        {
            out.writeInt(i);
        }

        public Integer deserialize(DataInputPlus in) throws IOException
        {
            return in.readInt();
        }

        public long serializedSize(Integer i)
        {
            return 4;
        }
    };

    private static final ISerializer<String> STRING_SERIALIZER = new ISerializer<>()
    {
        public void serialize(String v, DataOutputPlus out) throws IOException
        {
            out.writeUTF(v);
        }

        public String deserialize(DataInputPlus in) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String v)
        {
            return TypeSizes.sizeof(v);
        }
    };

    private static final IVersionedSerializer<IntervalTree<Integer, String, Interval<Integer, String>>> TREE_SERIALIZER = IntervalTree.serializer(INT_SERIALIZER, STRING_SERIALIZER, Interval::new);

    @Test
    public void testSearch()
    {
        List<Interval<Integer, Void>> intervals = new ArrayList<>();

        intervals.add(Interval.create(-300, -200));
        intervals.add(Interval.create(-3, -2));
        intervals.add(Interval.create(1, 2));
        intervals.add(Interval.create(3, 6));
        intervals.add(Interval.create(2, 4));
        intervals.add(Interval.create(5, 7));
        intervals.add(Interval.create(1, 3));
        intervals.add(Interval.create(4, 6));
        intervals.add(Interval.create(8, 9));
        intervals.add(Interval.create(15, 20));
        intervals.add(Interval.create(40, 50));
        intervals.add(Interval.create(49, 60));


        IntervalTree<Integer, Void, Interval<Integer, Void>> it = IntervalTree.build(intervals);

        assertEquals(3, it.search(Interval.create(4, 4)).size());
        assertEquals(4, it.search(Interval.create(4, 5)).size());
        assertEquals(7, it.search(Interval.create(-1, 10)).size());
        assertEquals(0, it.search(Interval.create(-1, -1)).size());
        assertEquals(5, it.search(Interval.create(1, 4)).size());
        assertEquals(2, it.search(Interval.create(0, 1)).size());
        assertEquals(0, it.search(Interval.create(10, 12)).size());

        List<Interval<Integer, Void>> intervals2 = new ArrayList<>();

        //stravinsky 1880-1971
        intervals2.add(Interval.create(1880, 1971));
        //Schoenberg
        intervals2.add(Interval.create(1874, 1951));
        //Grieg
        intervals2.add(Interval.create(1843, 1907));
        //Schubert
        intervals2.add(Interval.create(1779, 1828));
        //Mozart
        intervals2.add(Interval.create(1756, 1828));
        //Schuetz
        intervals2.add(Interval.create(1585, 1672));

        IntervalTree<Integer, Void, Interval<Integer, Void>> it2 = IntervalTree.build(intervals2);

        assertEquals(0, it2.search(Interval.create(1829, 1842)).size());

        List<Void> intersection1 = it2.search(Interval.create(1907, 1907));
        assertEquals(3, intersection1.size());

        intersection1 = it2.search(Interval.create(1780, 1790));
        assertEquals(2, intersection1.size());
    }

    @Test
    public void testIteration()
    {
        List<Interval<Integer, Void>> intervals = new ArrayList<>();

        intervals.add(Interval.create(-300, -200));
        intervals.add(Interval.create(-3, -2));
        intervals.add(Interval.create(1, 2));
        intervals.add(Interval.create(3, 6));
        intervals.add(Interval.create(2, 4));
        intervals.add(Interval.create(5, 7));
        intervals.add(Interval.create(1, 3));
        intervals.add(Interval.create(4, 6));
        intervals.add(Interval.create(8, 9));
        intervals.add(Interval.create(15, 20));
        intervals.add(Interval.create(40, 50));
        intervals.add(Interval.create(49, 60));

        IntervalTree<Integer, Void, Interval<Integer, Void>> it = IntervalTree.build(intervals);

        Collections.sort(intervals, Interval.minOrdering());

        List<Interval<Integer, Void>> l = ImmutableList.copyOf(it);

        assertEquals(intervals, l);
    }

    @Test
    public void testEmptyTree()
    {
        IntervalTree<Integer, String, Interval<Integer, String>> emptyTree = IntervalTree.emptyTree();

        assertTrue("Tree should be empty", emptyTree.isEmpty());
        assertEquals("Interval count should be 0", 0, emptyTree.intervalCount());

        try
        {
            emptyTree.min();
            fail("Expected IllegalStateException when calling min() on empty tree");
        }
        catch (IllegalStateException e) { /* expected */ }

        try
        {
            emptyTree.max();
            fail("Expected IllegalStateException when calling max() on empty tree");
        }
        catch (IllegalStateException e) { /* expected */ }

        assertTrue("Search should yield empty list for empty tree",
                   emptyTree.search(Interval.create(1, 5, "data")).isEmpty());
        assertTrue("Search by point should yield empty list for empty tree",
                   emptyTree.search(5).isEmpty());

        assertFalse("Iterator should have no elements", emptyTree.iterator().hasNext());
    }

    @Test
    public void testSingleInterval()
    {
        Interval<Integer, String> interval = Interval.create(10, 20, "single");
        List<Interval<Integer, String>> list = new ArrayList<>();
        list.add(interval);

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(list);

        assertFalse("Tree should not be empty", tree.isEmpty());
        assertEquals("Interval count should be 1", 1, tree.intervalCount());
        assertEquals("min() should match the only interval's min", Integer.valueOf(10), tree.min());
        assertEquals("max() should match the only interval's max", Integer.valueOf(20), tree.max());

        List<String> result = tree.search(Interval.create(10, 20));
        assertEquals("Should find exactly 1 match", 1, result.size());
        assertEquals("Data should match 'single'", "single", result.get(0));

        result = tree.search(Interval.create(15, 25));
        assertEquals("Should find overlap", 1, result.size());

        result = tree.search(Interval.create(1, 9));
        assertTrue("Should find no intervals that do not overlap", result.isEmpty());

        List<Interval<Integer, String>> iterationList = ImmutableList.copyOf(tree);

        assertEquals("Iteration should produce exactly our single interval",
                     list, iterationList);
    }

    @Test
    public void testMultipleIntervals()
    {
        List<Interval<Integer, String>> intervals = new ArrayList<>();
        intervals.add(Interval.create(1, 3, "A"));
        intervals.add(Interval.create(2, 4, "B"));
        intervals.add(Interval.create(5, 7, "C"));
        intervals.add(Interval.create(6, 6, "D"));   // single-point overlap within (5,7)
        intervals.add(Interval.create(8, 10, "E"));
        intervals.add(Interval.create(10, 12, "F")); // boundary adjacency with (8,10)

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(intervals);

        assertFalse("Tree should not be empty", tree.isEmpty());
        assertEquals("Interval count", intervals.size(), tree.intervalCount());

        assertEquals("min()", Integer.valueOf(1), tree.min());
        assertEquals("max()", Integer.valueOf(12), tree.max());

        List<String> result = tree.search(Interval.create(2, 2)); // point in [1,3] and also in [2,4]
        assertTrue(result.contains("A"));
        assertTrue(result.contains("B"));
        assertEquals("Should find 2 intervals for point=2", 2, result.size());

        result = tree.search(Interval.create(4, 6));
        assertTrue(result.contains("B"));
        assertTrue(result.contains("C"));
        assertTrue(result.contains("D"));
        assertEquals("Should have 3 overlaps for [4,6]", 3, result.size());

        result = tree.search(Interval.create(13, 14));
        assertTrue("Should be no matches for [13,14]", result.isEmpty());

        result = tree.search(Interval.create(10, 10));
        assertTrue(result.contains("E"));
        assertTrue(result.contains("F"));
        assertEquals("Should find 2 intervals at boundary=10", 2, result.size());

        Collections.sort(intervals, Interval.minOrdering());
        List<Interval<Integer, String>> iterated = ImmutableList.copyOf(tree);
        assertEquals("Iterated intervals should be in ascending min-order", intervals, iterated);
    }

    @Test
    public void testDuplicateAndSameBoundaryIntervals()
    {
        List<Interval<Integer, String>> intervals = new ArrayList<>();
        intervals.add(Interval.create(5, 5, "X"));  // single point
        intervals.add(Interval.create(5, 5, "Y"));  // same single point, different data
        intervals.add(Interval.create(5, 5, "X"));  // same data and boundary as first
        intervals.add(Interval.create(5, 6, "Z"));  // partial overlap
        intervals.add(Interval.create(4, 5, "W"));  // partial overlap at boundary

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(intervals);

        assertEquals("min()", Integer.valueOf(4), tree.min());
        assertEquals("max()", Integer.valueOf(6), tree.max());

        List<String> result = tree.search(5);
        assertEquals("Should have 5 matching intervals that contain point=5", 5, result.size());

        result = tree.search(Interval.create(5, 5));
        assertEquals("Should have 5 matching intervals that contain [5,5]", 5, result.size());

        result = tree.search(Interval.create(6, 6));
        assertEquals("Should match only [5,6] for point=6", 1, result.size());
        assertTrue(result.contains("Z"));

        result = tree.search(7);
        assertTrue("No intervals should contain point=7", result.isEmpty());
    }

    @Test
    public void testEqualsAndHashCode()
    {
        IntervalTree<Integer, String, Interval<Integer, String>> tree1 = IntervalTree.build(ImmutableList.of());
        IntervalTree<Integer, String, Interval<Integer, String>> tree2 = IntervalTree.build(ImmutableList.of());

        try
        {
            tree1.equals(tree2);
            fail("Expected UnsupportedOperationException when calling equals");
        }
        catch (UnsupportedOperationException e) { /* expected */ }

        try
        {
            tree1.hashCode();
            fail("Expected UnsupportedOperationException when calling hashCode");
        }
        catch (UnsupportedOperationException e) { /* expected */ }
    }

    @Test
    public void testSerialization() throws Exception
    {
        List<Interval<Integer, String>> intervals = new ArrayList<>();
        intervals.add(Interval.create(-300, -200, "a"));
        intervals.add(Interval.create(-3, -2, "b"));
        intervals.add(Interval.create(1, 2, "c"));
        intervals.add(Interval.create(1, 3, "d"));
        intervals.add(Interval.create(2, 4, "e"));
        intervals.add(Interval.create(3, 6, "f"));
        intervals.add(Interval.create(4, 6, "g"));
        intervals.add(Interval.create(5, 7, "h"));
        intervals.add(Interval.create(8, 9, "i"));
        intervals.add(Interval.create(15, 20, "j"));
        intervals.add(Interval.create(40, 50, "k"));
        intervals.add(Interval.create(49, 60, "l"));

        IntervalTree<Integer, String, Interval<Integer, String>> it = IntervalTree.build(intervals);

        DataOutputBuffer out = new DataOutputBuffer();
        TREE_SERIALIZER.serialize(it, out, 0);

        DataInputPlus in = new DataInputBuffer(out.toByteArray());
        IntervalTree<Integer, String, Interval<Integer, String>> itDeserialized = TREE_SERIALIZER.deserialize(in, 0);

        List<Interval<Integer, String>> intervals2 = ImmutableList.copyOf(itDeserialized);

        assertEquals("Interval lists should match after deserialization", intervals, intervals2);
    }

    @Test
    public void testPointSearchEquivalence()
    {
        List<Interval<Integer, String>> intervals = new ArrayList<>();
        intervals.add(Interval.create(1, 3, "A"));
        intervals.add(Interval.create(2, 5, "B"));
        intervals.add(Interval.create(10, 20, "C"));

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(intervals);

        List<String> resultPoint = tree.search(2);
        List<String> resultInterval = tree.search(Interval.create(2, 2));

        assertEquals("Results of point search and interval-based search should match",
                     resultPoint, resultInterval);
    }

    private Gen<Interval<Integer, String>> intervalGen()
    {
        return SourceDSL.integers().between(-5, 5)
                        .flatMap(start ->
                                 SourceDSL.integers().between(-5, 5)
                                          .map(end -> {
                                              int lo = Math.min(start, end);
                                              int hi = Math.max(start, end);
                                              String data = "(" + lo + "," + hi + ")";
                                              return Interval.create(lo, hi, data);
                                          }));
    }

    private Gen<List<Interval<Integer, String>>> intervalsListGen()
    {
        return lists().of(intervalGen())
                      .ofSizeBetween(0, 30);
    }

    private Gen<Interval<Integer, String>> queryGen()
    {
        return SourceDSL.booleans().all()
                        .flatMap(isPoint -> {
                            if (isPoint)
                                return SourceDSL.integers().between(-5, 5)
                                                .map(x -> Interval.create(x, x, "queryPoint(" + x + ")"));
                            else
                                return intervalGen().map(i -> Interval.create(i.min, i.max, "query[" + i.min + "," + i.max + "]"));
                        });
    }

    private boolean overlaps(Interval<Integer, ?> a, Interval<Integer, ?> b)
    {
        return a.min <= b.max && a.max >= b.min;
    }

    private <D> List<D> search(Collection<Interval<Integer, D>> intervals, Interval<Integer, ?> query)
    {
        List<D> results = new ArrayList<>();
        for (Interval<Integer, D> candidate : intervals)
        {
            if (overlaps(candidate, query))
                results.add(candidate.data);
        }
        return results;
    }

    @Test
    public void qtIntervalTreeTest()
    {
        qt().forAll(intervalsListGen(), queryGen(), SourceDSL.booleans().all())
            .check((intervals, query, addOrBuild) -> {
                Set<Interval<Integer, String>> intervalsSet = ImmutableSet.copyOf(intervals);
                IntervalTree<Integer, String, Interval<Integer, String>> tree;
                if (addOrBuild)
                {
                    tree = IntervalTree.build(ImmutableList.of());
                    for (Interval<Integer, String> interval : intervals)
                        tree = tree.add(ImmutableList.of(interval));
                }
                else
                {
                    tree = IntervalTree.build(intervals);
                }

                List<String> expected = search(intervals, query);
                List<String> actual = tree.search(query);

                Set<String> setExpected = new HashSet<>(expected);
                Set<String> setActual = new HashSet<>(actual);

                assertEquals(setExpected, setActual);

                if (query.min.equals(query.max))
                {
                    List<String> actualPoint = tree.search(query.min);
                    assertEquals(setExpected, new HashSet<>(actualPoint));
                }

                List<Interval<Integer, String>> sortedByMin = new ArrayList<>(intervals);
                sortedByMin.sort(Interval.minOrdering());

                Set<Interval<Integer, String>> fromTree = ImmutableSet.copyOf(tree);
                assertEquals(intervalsSet, fromTree);

                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    TREE_SERIALIZER.serialize(tree, out, 0);
                    IntervalTree<Integer, String, Interval<Integer, String>> roundTrip;
                    try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
                    {
                        roundTrip = TREE_SERIALIZER.deserialize(in, 0);
                    }

                    Set<Interval<Integer, String>> roundTripIntervals = ImmutableSet.copyOf(roundTrip);
                    assertEquals(fromTree, roundTripIntervals);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                return true;
            });
    }


    @Test
    public void testCopyAndAddIntervals()
    {
        List<Interval<Integer, Void>> intervals = new ArrayList<>();

        intervals.add(Interval.create(-300, -200));
        intervals.add(Interval.create(-3, -2));
        intervals.add(Interval.create(1, 2));
        intervals.add(Interval.create(3, 6));
        intervals.add(Interval.create(2, 4));
        intervals.add(Interval.create(5, 7));
        intervals.add(Interval.create(4, 6));
        intervals.add(Interval.create(15, 20));
        intervals.add(Interval.create(49, 60));


        IntervalTree<Integer, Void, Interval<Integer, Void>> it = IntervalTree.build(intervals);

        List<Interval<Integer, Void>> intervalsToAdd = new ArrayList<>();
        intervalsToAdd.add(Interval.create(1, 3));
        intervalsToAdd.add(Interval.create(8, 9));
        intervalsToAdd.add(Interval.create(40, 50));

        it = it.add(intervalsToAdd);

        assertEquals(3, it.search(Interval.create(4, 4)).size());
        assertEquals(4, it.search(Interval.create(4, 5)).size());
        assertEquals(7, it.search(Interval.create(-1, 10)).size());
        assertEquals(0, it.search(Interval.create(-1, -1)).size());
        assertEquals(5, it.search(Interval.create(1, 4)).size());
        assertEquals(2, it.search(Interval.create(0, 1)).size());
        assertEquals(0, it.search(Interval.create(10, 12)).size());
    }
}
