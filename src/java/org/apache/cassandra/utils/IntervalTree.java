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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.AsymmetricOrdering.Op;

public class IntervalTree<C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> implements Iterable<I>
{
    private static final Logger logger = LoggerFactory.getLogger(IntervalTree.class);

    private static final Interval[] EMPTY_ARRAY = new Interval[0];

    @SuppressWarnings("unchecked")
    private static final IntervalTree EMPTY_TREE = new IntervalTree(null);

    private final IntervalNode head;
    private final I[] intervalsByMinOrder;
    private final I[] intervalsByMaxOrder;

    protected IntervalTree(Collection<I> intervals)
    {
        if (intervals == null || intervals.isEmpty())
        {
            this.head = null;
            intervalsByMinOrder = intervalsByMaxOrder = (I[])EMPTY_ARRAY;
        }
        else if (intervals.size() == 1)
        {
            intervalsByMinOrder = intervalsByMaxOrder = (I[])new Interval[] { intervals.iterator().next() };
            this.head = new IntervalNode(intervals);
        }
        else
        {
            List<I> minSortedIntervals = new ArrayList<>(intervals);
            Collections.sort(minSortedIntervals, Interval.minOrdering());
            intervalsByMinOrder = (I[])new Interval[minSortedIntervals.size()];
            minSortedIntervals.toArray(intervalsByMinOrder);
            List<I> maxSortedIntervals = new ArrayList<>(intervals);
            Collections.sort(maxSortedIntervals, Interval.maxOrdering());
            intervalsByMaxOrder = (I[])new Interval[maxSortedIntervals.size()];
            maxSortedIntervals.toArray(intervalsByMaxOrder);
            this.head = new IntervalNode(minSortedIntervals, maxSortedIntervals);
        }
    }

    protected IntervalTree(I[] minSortedIntervals, I[] maxSortedIntervals)
    {
        if (minSortedIntervals == null || minSortedIntervals.length == 0)
        {
            this.head = null;
            intervalsByMinOrder = intervalsByMaxOrder = (I[])EMPTY_ARRAY;
        }
        else if (minSortedIntervals.length == 1)
        {
            intervalsByMinOrder = intervalsByMaxOrder = minSortedIntervals;
            List<I> intervals = Collections.singletonList(minSortedIntervals[0]);
            this.head = new IntervalNode(intervals, intervals);
        }
        else
        {
            intervalsByMinOrder = minSortedIntervals;
            intervalsByMaxOrder = maxSortedIntervals;
            this.head = new IntervalNode(Arrays.asList(minSortedIntervals), Arrays.asList(maxSortedIntervals));
        }
    }

    public static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> IntervalTree<C, D, I> build(Collection<I> intervals)
    {
        if (intervals == null || intervals.isEmpty())
            return emptyTree();

        return new IntervalTree<C, D, I>(intervals);
    }

    @SuppressWarnings("unchecked")
    public static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> IntervalTree<C, D, I> emptyTree()
    {
        return EMPTY_TREE;
    }

    public int intervalCount()
    {
        return intervalsByMinOrder.length;
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

    public IntervalTree<C, D, I> update(List<I> removals, List<I> additions)
    {
        if ((removals == null || removals.isEmpty()) && (additions == null || additions.isEmpty()))
        {
            return this;
        }

        I[] removalsArray = removals.toArray((I[])EMPTY_ARRAY);
        I[] additionsArray = additions.toArray((I[])EMPTY_ARRAY);
        Arrays.sort(removalsArray, Interval.<C, D>minOrdering());
        Arrays.sort(additionsArray, Interval.<C, D>minOrdering());

        I[] newByMin = buildUpdatedArray(
            intervalsByMinOrder,
            removalsArray,
            additionsArray,
            Interval.<C, D>minOrdering()
        );

        Arrays.sort(removalsArray, Interval.<C, D>maxOrdering());
        Arrays.sort(additionsArray, Interval.<C, D>maxOrdering());

        I[] newByMax = buildUpdatedArray(
            intervalsByMaxOrder,
            removalsArray,
            additionsArray,
            Interval.<C, D>maxOrdering()
        );

        return new IntervalTree<C, D, I>(newByMin, newByMax);
    }

    @SuppressWarnings("unchecked")
    private I[] buildUpdatedArray(I[] existingSorted,
                                  I[] removalsSorted,
                                  I[] additionsSorted,
                                  AsymmetricOrdering<Interval<C, D>, C> cmp)
    {
        int finalSize = existingSorted.length + additionsSorted.length - removalsSorted.length;
        I[] result = (I[]) new Interval[finalSize];

        int existingIndex  = 0;
        int removalsIndex  = 0;
        int additionsIndex = 0;
        int resultIndex    = 0;

        while (existingIndex < existingSorted.length)
        {
            I currentExisting = existingSorted[existingIndex];

            while (removalsIndex < removalsSorted.length
                   && cmp.compare(removalsSorted[removalsIndex], currentExisting) <= 0)
            {
                int c = cmp.compare(removalsSorted[removalsIndex], currentExisting);
                if (c < 0)
                {
                    throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[removalsIndex]);
                }
                else
                {
                    existingIndex++;
                    removalsIndex++;

                    if (existingIndex >= existingSorted.length)
                        break;
                    currentExisting = existingSorted[existingIndex];
                }
            }

            if (existingIndex >= existingSorted.length )
                break;

            while (additionsIndex < additionsSorted.length
                   && cmp.compare(additionsSorted[additionsIndex], currentExisting) <= 0)
            {
                result[resultIndex++] = additionsSorted[additionsIndex++];
            }

            result[resultIndex++] = currentExisting;
            existingIndex++;
        }

        if (removalsIndex < removalsSorted.length)
            throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[removalsIndex]);

        while (additionsIndex < additionsSorted.length)
            result[resultIndex++] = additionsSorted[additionsIndex++];

        return result;
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
        if(!(o instanceof IntervalTree))
            return false;
        IntervalTree that = (IntervalTree)o;
        return Iterators.elementsEqual(iterator(), that.iterator());
    }

    @Override
    public final int hashCode()
    {
        int result = 0;
        for (Interval<C, D> interval : this)
            result = 31 * result + interval.hashCode();
        return result;
    }

    private class IntervalNode
    {
        final C center;
        final C low;
        final C high;

        final List<I> intersectsLeft;
        final List<I> intersectsRight;

        final IntervalNode left;
        final IntervalNode right;

        public IntervalNode(Collection<I> toBisect)
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

        public IntervalNode(List<I> minOrder, List<I> maxOrder)
        {
            assert !minOrder.isEmpty();
            logger.trace("Creating IntervalNode from {}", minOrder);

            // Building IntervalTree with one interval will be a reasonably
            // common case for range tombstones, so it's worth optimizing
            if (minOrder.size() == 1)
            {
                I interval = minOrder.iterator().next();
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
            List<I> leftSegmentMinOrder = new ArrayList<I>();
            List<I> leftSegmentMaxOrder = new ArrayList<>();
            List<I> rightSegmentMinOrder = new ArrayList<I>();
            List<I> rightSegmentMaxOrder = new ArrayList<>();

            for (I candidate : minOrder)
            {
                if (candidate.max.compareTo(center) < 0)
                    leftSegmentMinOrder.add(candidate);
                else if (candidate.min.compareTo(center) > 0)
                    rightSegmentMinOrder.add(candidate);
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

            left = leftSegmentMinOrder.isEmpty() ? null : new IntervalNode(leftSegmentMinOrder, leftSegmentMaxOrder);
            right = rightSegmentMinOrder.isEmpty() ? null : new IntervalNode(rightSegmentMinOrder, rightSegmentMaxOrder);

            assert (intersectsLeft.size() == intersectsRight.size());
            assert (intersectsLeft.size() + leftSegmentMinOrder.size() + rightSegmentMinOrder.size()) == minOrder.size() :
                    "intersects (" + String.valueOf(intersectsLeft.size()) +
                            ") + leftSegment (" + String.valueOf(leftSegmentMinOrder.size()) +
                            ") + rightSegment (" + String.valueOf(rightSegmentMinOrder.size()) +
                            ") != toBisect (" + String.valueOf(minOrder.size()) + ")";
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
                int j = Interval.<C, D>minOrdering().binarySearchAsymmetric(intersectsLeft, searchInterval.max, Op.HIGHER);
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
}
