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
package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AsymmetricOrdering.Op;

import static org.apache.cassandra.utils.AsymmetricOrdering.Op.HIGHER;

public class IntervalTree<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements Iterable<I>
{
    private static final Logger logger = LoggerFactory.getLogger(IntervalTree.class);
    private static final int REBUILD_AT_MOD_COUNT = 20;

    @SuppressWarnings("unchecked")
    private static final IntervalTree EMPTY_TREE = new IntervalTree(null);

    protected final IntervalNode<C, D, I> head;
    protected final int count;
    protected final int modCount;

    protected IntervalTree(int count, int modCount, IntervalNode<C, D, I> head)
    {
        this.head = head;
        this.count = count;
        this.modCount = modCount;
    }

    protected IntervalTree(Collection<I> intervals)
    {
        this.modCount = 0;
        if (intervals == null || intervals.isEmpty())
        {
            this.head = null;
            this.count = 0;
        }
        else if (intervals.size() == 1)
        {
            this.head = new IntervalNode(intervals);
            this.count = intervals.size();
        }
        else
        {
            List<I> minSortedIntervals = new ArrayList<>(intervals);
            Collections.sort(minSortedIntervals, Interval.minOrdering());
            List<I> maxSortedIntervals = new ArrayList<>(intervals);
            Collections.sort(maxSortedIntervals, Interval.maxOrdering());
            this.head = new IntervalNode(minSortedIntervals, minSortedIntervals, maxSortedIntervals);
            this.count = intervals.size();
        }
    }

    public IntervalTree<C, D, I> add(List<I> intervals)
    {
        if (head == null)
            return create(intervals);
        if (modCount + 1 >= REBUILD_AT_MOD_COUNT)
        {
            return create(new AbstractCollection<I>()
            {
                @Override
                public Iterator<I> iterator()
                {
                    return Iterators.concat(IntervalTree.this.iterator(), intervals.iterator());
                }

                @Override
                public int size()
                {
                    return count + intervals.size();
                }
            });
        }
        return create(count + intervals.size(), modCount + 1, head.add(intervals));
    }

    protected IntervalTree<C, D, I> create(int count, int modCount, IntervalNode<C, D, I> head)
    {
        return new IntervalTree<>(count, modCount, head);
    }

    protected IntervalTree<C, D, I> create(Collection<I> intervals)
    {
        return new IntervalTree<>(intervals);
    }

    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> build(Collection<I> intervals)
    {
        if (intervals == null || intervals.isEmpty())
            return emptyTree();

        return new IntervalTree<C, D, I>(intervals);
    }

    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> Serializer<C, D, I> serializer(ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, TriFunction<C, C, D, I> constructor)
    {
        return new Serializer<>(pointSerializer, dataSerializer, constructor);
    }

    @SuppressWarnings("unchecked")
    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> emptyTree()
    {
        return EMPTY_TREE;
    }

    public int intervalCount()
    {
        return count;
    }

    public boolean isEmpty()
    {
        return head == null;
    }

    public C max()
    {
        if (head == null)
            throw new IllegalStateException();

        return head.high;
    }

    public C min()
    {
        if (head == null)
            throw new IllegalStateException();

        return head.low;
    }

    public List<D> search(Interval<C, D> searchInterval)
    {
        if (head == null)
            return Collections.<D>emptyList();

        List<D> results = new ArrayList<D>();
        head.searchInternal(searchInterval, results);
        return results;
    }

    public List<D> search(C point)
    {
        return search(Interval.<C, D>create(point, point, null));
    }

    public Iterator<I> iterator()
    {
        if (head == null)
            return Collections.emptyIterator();

        return new TreeIterator(head);
    }

    @Override
    public String toString()
    {
        return "<" + Joiner.on(", ").join(this) + ">";
    }

    @Override
    public boolean equals(Object o)
    {
        // This is deceptively not useful because the iteration order depends on input order and we don't sort
        // on all the fields in minOrder and maxOrder, and for performance we don't want to
        throw new UnsupportedOperationException();
    }

    @Override
    public final int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    protected static class IntervalNode<C extends Comparable<? super C>, D, I extends Interval<C, D>>
    {
        final C center;
        final C low;
        final C high;

        final List<I> intersectsLeft;
        final List<I> intersectsRight;

        final IntervalNode left;
        final IntervalNode right;

        private IntervalNode(Collection<I> toBisect)
        {
            assert toBisect.size() == 1;
            I interval = toBisect.iterator().next();
            low = interval.min;
            center = interval.max;
            high = interval.max;
            List<I> l = Collections.singletonList(interval);
            intersectsLeft = l;
            intersectsRight = l;
            left = null;
            right = null;
        }

        private IntervalNode(List<I> toBisect, List<I> minOrder, List<I> maxOrder)
        {
            assert !toBisect.isEmpty();
            logger.trace("Creating IntervalNode from {}", toBisect);

            // Building IntervalTree with one interval will be a reasonably
            // common case for range tombstones, so it's worth optimizing
            if (toBisect.size() == 1)
            {
                I interval = toBisect.iterator().next();
                low = interval.min;
                center = interval.max;
                high = interval.max;
                List<I> l = Collections.singletonList(interval);
                intersectsLeft = l;
                intersectsRight = l;
                left = null;
                right = null;
                return;
            }

            low = minOrder.get(0).min;
            high = maxOrder.get(maxOrder.size() - 1).max;

            int totalPoints = minOrder.size() * 2;
            int midIndex = totalPoints / 2;
            int i = 0, j = 0, count = 0;
            while (count < midIndex)
            {
                if (i < minOrder.size() && (j >= maxOrder.size() || minOrder.get(i).min.compareTo(maxOrder.get(j).max) <= 0))
                    i++;
                else
                    j++;
                count++;
            }

            if (i < minOrder.size() && (j >= maxOrder.size() || minOrder.get(i).min.compareTo(maxOrder.get(j).max) < 0))
                center = minOrder.get(i).min;
            else
                center = maxOrder.get(j).max;

            // Separate interval in intersecting center, left of center and right of center
            intersectsLeft = new ArrayList<I>();
            intersectsRight = new ArrayList<I>();
            List<I> leftSegment = new ArrayList<I>();
            List<I> leftSegmentMaxOrder = new ArrayList<>();
            List<I> rightSegment = new ArrayList<I>();
            List<I> rightSegmentMaxOrder = new ArrayList<>();

            for (I candidate : minOrder)
            {
                if (candidate.max.compareTo(center) < 0)
                    leftSegment.add(candidate);
                else if (candidate.min.compareTo(center) > 0)
                    rightSegment.add(candidate);
                else
                    intersectsLeft.add(candidate);
            }

            for (I candidate : maxOrder)
            {
                if (candidate.max.compareTo(center) < 0)
                    leftSegmentMaxOrder.add(candidate);
                else if (candidate.min.compareTo(center) > 0)
                    rightSegmentMaxOrder.add(candidate);
                else
                    intersectsRight.add(candidate);
            }

            left = leftSegment.isEmpty() ? null : new IntervalNode(leftSegment, leftSegment, leftSegmentMaxOrder);
            right = rightSegment.isEmpty() ? null : new IntervalNode(rightSegment, rightSegment, rightSegmentMaxOrder);

            assert (intersectsLeft.size() == intersectsRight.size());
            assert (intersectsLeft.size() + leftSegment.size() + rightSegment.size()) == toBisect.size() :
                    "intersects (" + String.valueOf(intersectsLeft.size()) +
                            ") + leftSegment (" + String.valueOf(leftSegment.size()) +
                            ") + rightSegment (" + String.valueOf(rightSegment.size()) +
                            ") != toBisect (" + String.valueOf(toBisect.size()) + ")";
        }

        private IntervalNode(C center, C low, C high, List<I> intersectsLeft, List<I> intersectsRight, IntervalNode left, IntervalNode right)
        {
            this.center = center;
            this.low = low;
            this.high = high;
            this.intersectsLeft = intersectsLeft;
            this.intersectsRight = intersectsRight;
            this.left = left;
            this.right = right;
        }

        void searchInternal(Interval<C, D> searchInterval, List<D> results)
        {
            if (center.compareTo(searchInterval.min) < 0)
            {
                int i = Interval.<C, D>maxOrdering().binarySearchAsymmetric(intersectsRight, searchInterval.min, Op.CEIL);
                if (i == intersectsRight.size() && high.compareTo(searchInterval.min) < 0)
                    return;

                while (i < intersectsRight.size())
                    results.add(intersectsRight.get(i++).data);

                if (right != null)
                    right.searchInternal(searchInterval, results);
            }
            else if (center.compareTo(searchInterval.max) > 0)
            {
                int j = Interval.<C, D>minOrdering().binarySearchAsymmetric(intersectsLeft, searchInterval.max, HIGHER);
                if (j == 0 && low.compareTo(searchInterval.max) > 0)
                    return;

                for (int i = 0 ; i < j ; i++)
                    results.add(intersectsLeft.get(i).data);

                if (left != null)
                    left.searchInternal(searchInterval, results);
            }
            else
            {
                // Adds every interval contained in this node to the result set then search left and right for further
                // overlapping intervals
                for (Interval<C, D> interval : intersectsLeft)
                    results.add(interval.data);

                if (left != null)
                    left.searchInternal(searchInterval, results);
                if (right != null)
                    right.searchInternal(searchInterval, results);
            }
        }

        private IntervalNode<C, D, I> add(List<I> intervals)
        {
            return add(this, intervals);
        }

        private static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalNode<C, D, I> add(IntervalNode<C, D, I> root, List<I> intervals)
        {
            if (intervals.isEmpty())
                return root;

            if (root == null)
            {
                List<I> minSortedIntervals = new ArrayList<>(intervals);
                Collections.sort(minSortedIntervals, Interval.minOrdering());
                List<I> maxSortedIntervals = new ArrayList<>(intervals);
                Collections.sort(maxSortedIntervals, Interval.maxOrdering());
                return new IntervalNode(intervals, minSortedIntervals, maxSortedIntervals);
            }

            List<I> leftSegment = new ArrayList<>();
            List<I> rightSegment = new ArrayList<>();
            C newLow = root.low;
            C newHigh = root.high;
            List<I> newIntersectsLeft = new ArrayList<>(root.intersectsLeft);
            List<I> newIntersectsRight = new ArrayList<>(root.intersectsRight);
            for (I i : intervals)
            {
                newLow = newLow.compareTo(i.min) < 0 ? newLow : i.min;
                newHigh = newHigh.compareTo(i.max) > 0 ? newHigh : i.max;
                if (i.max.compareTo(root.center) < 0)
                {
                    leftSegment.add(i);
                }
                else if (i.min.compareTo(root.center) > 0)
                {
                    rightSegment.add(i);
                }
                else
                {
                    int leftIdx = Interval.<C, D>minOrdering().binarySearchAsymmetric(newIntersectsLeft, i.min, Op.CEIL);
                    newIntersectsLeft.add(leftIdx, i);

                    int rightIdx = Interval.<C, D>maxOrdering().binarySearchAsymmetric(newIntersectsRight, i.max, Op.HIGHER);
                    newIntersectsRight.add(rightIdx, i);
                }
            }

            return new IntervalNode(root.center,
                                    newLow,
                                    newHigh,
                                    newIntersectsLeft,
                                    newIntersectsRight,
                                    add(root.left, leftSegment),
                                    add(root.right, rightSegment));
        }
    }

    private class TreeIterator extends AbstractIterator<I>
    {
        private final Deque<IntervalNode> stack = new ArrayDeque<IntervalNode>();
        private Iterator<I> current;

        TreeIterator(IntervalNode node)
        {
            super();
            gotoMinOf(node);
        }

        protected I computeNext()
        {
            while (true)
            {
                if (current != null && current.hasNext())
                    return current.next();

                IntervalNode node = stack.pollFirst();
                if (node == null)
                    return endOfData();

                current = node.intersectsLeft.iterator();

                // We know this is the smaller not returned yet, but before doing
                // its parent, we must do everyone on it's right.
                gotoMinOf(node.right);
            }
        }

        private void gotoMinOf(IntervalNode node)
        {
            while (node != null)
            {
                stack.offerFirst(node);
                node = node.left;
            }

        }
    }

    public static class Serializer<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements IVersionedSerializer<IntervalTree<C, D, I>>
    {
        private final ISerializer<C> pointSerializer;
        private final ISerializer<D> dataSerializer;
        private final TriFunction<C, C, D, I> constructor;

        private Serializer(ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, TriFunction<C, C, D, I> constructor)
        {
            this.pointSerializer = pointSerializer;
            this.dataSerializer = dataSerializer;
            this.constructor = constructor;
        }

        public void serialize(IntervalTree<C, D, I> it, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(it.count);
            for (Interval<C, D> interval : it)
            {
                pointSerializer.serialize(interval.min, out);
                pointSerializer.serialize(interval.max, out);
                dataSerializer.serialize(interval.data, out);
            }
        }

        /**
         * Deserialize an IntervalTree whose keys use the natural ordering.
         * Use deserialize(DataInput, int, Comparator) instead if the interval
         * tree is to use a custom comparator, as the comparator is *not*
         * serialized.
         */
        public IntervalTree<C, D, I> deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in, version, null);
        }

        public IntervalTree<C, D, I> deserialize(DataInputPlus in, int version, Comparator<C> comparator) throws IOException
        {
            int count = in.readInt();
            List<I> intervals = new ArrayList<I>(count);
            for (int i = 0; i < count; i++)
            {
                C min = pointSerializer.deserialize(in);
                C max = pointSerializer.deserialize(in);
                D data = dataSerializer.deserialize(in);
                intervals.add(constructor.apply(min, max, data));
            }
            return new IntervalTree<C, D, I>(intervals);
        }

        public long serializedSize(IntervalTree<C, D, I> it, int version)
        {
            long size = TypeSizes.sizeof(0);
            for (Interval<C, D> interval : it)
            {
                size += pointSerializer.serializedSize(interval.min);
                size += pointSerializer.serializedSize(interval.max);
                size += dataSerializer.serializedSize(interval.data);
            }
            return size;
        }
    }
}
