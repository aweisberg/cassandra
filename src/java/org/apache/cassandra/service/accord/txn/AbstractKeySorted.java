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

package org.apache.cassandra.service.accord.txn;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static com.google.common.base.Preconditions.checkState;

/**
 * Immutable collection of items, sorted first by their partition key
 */
public abstract class AbstractKeySorted<T> implements Iterable<T>
{
    public static final String ITEMS_OUT_OF_ORDER_MESSAGE = "Items are out of order ([%s] %s >= [%s] %s)";

    protected final Seekables itemKeys;
    protected final T[] items;

    public AbstractKeySorted(T[] items)
    {
        this.items = items;
        this.itemKeys = extractItemKeys();
    }

    public AbstractKeySorted(List<T> items)
    {
        T[] arr = newArray(items.size());
        items.toArray(arr);
        this.items = arr;
        if (items.size() == 0)
        {
            this.itemKeys = Keys.of();
            return;
        }
        Domain domain = getKeys(arr[0]).domain();
        switch (domain)
        {
            case Key:
                Arrays.sort(arr, this::compareKey);
                break;
            case Range:
                Arrays.sort(arr, this::compareRange);
                break;
            default:
                throw new IllegalStateException("Unhandled domain " + domain);
        }
        this.itemKeys = extractItemKeys();
    }

    private Seekables extractItemKeys()
    {
        // TODO (review): This doesn't pick the "right" domain which could make Accord angry (in practice it doesn't)
        // but I think the right track going forward is to be selectively forgiving of empty Seekables in the wrong
        // domain in Accord for `Update.key()` and `Read.keys` to save implementations from having to propagate them
        // or alternatively just allow null `Read` just like we allow null `Query` and null `Update`
        if (items.length == 0)
            return Keys.of();

        Domain domain = getKeys(items[0]).domain();
        int totalKeys = 0;
        for (int i = 0; i < items.length; i++)
            totalKeys += getKeys(items[i]).size();
        switch (domain)
        {
            case Key:
                PartitionKey[] keys = new PartitionKey[totalKeys];
                for (int i = 0 ; i < keys.length;)
                {
                    Keys itemKeys = (Keys)getKeys(items[i]);
                    for (int j = 0; j < itemKeys.size(); j++)
                        keys[i++] = (PartitionKey) itemKeys.get(j);
                }
                return Keys.ofSorted(keys);
            case Range:
                TokenRange[] ranges = new TokenRange[totalKeys];
                for (int i = 0 ; i < ranges.length;)
                {
                    Ranges itemRanges = (Ranges)getKeys(items[i]);
                    for (int j = 0; j < itemRanges.size(); j++)
                        ranges[i++] = (TokenRange) itemRanges.get(j);
                }
                return Ranges.ofSortedAndDeoverlapped(ranges);
            default:
                throw new IllegalStateException("Unhandled domain " + domain);
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return Iterators.forArray(items);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + Arrays.stream(items)
                                                  .map(Objects::toString)
                                                  .collect(Collectors.joining(", ", "{", "}"));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKeySorted<?> that = (AbstractKeySorted<?>) o;
        return Arrays.equals(items, that.items);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(items);
    }

    @VisibleForTesting
    public Seekables keys()
    {
        return itemKeys;
    }

    /**
     * Compare the non-key component of items (since this class handles sorting by key)
     */
    abstract int compareNonKeyFields(T left, T right);

    abstract Seekables getKeys(T item);
    abstract T[] newArray(int size);

    public int compareKey(T left, T right)
    {
        int cmp = ((PartitionKey)getKeys(left).get(0)).compareTo(((PartitionKey)getKeys(right).get(0)));
        return cmp != 0 ? cmp : compareNonKeyFields(left, right);
    }

    public int compareRange(T left, T right)
    {
        int cmp = ((TokenRange)getKeys(left).get(0)).compareTo(((TokenRange)getKeys(right).get(0)));
        return cmp != 0 ? cmp : compareNonKeyFields(left, right);
    }

    @VisibleForTesting
    void validateOrder()
    {
        Domain domain = getKeys(items[0]).domain();
        switch (domain)
        {
            case Key:
                for (int i = 1; i < items.length; i++)
                {
                    T prev = items[i-1];
                    T next = items[i];

                    if (compareKey(prev, next) >= 0)
                        throw new IllegalStateException(String.format(ITEMS_OUT_OF_ORDER_MESSAGE, i - 1, prev, i, next));
                }
            case Range:
                for (int i = 1; i < items.length; i++)
                {
                    T prev = items[i-1];
                    T next = items[i];

                    if (compareRange(prev, next) >= 0)
                        throw new IllegalStateException(String.format(ITEMS_OUT_OF_ORDER_MESSAGE, i - 1, prev, i, next));
                }
            default:
                throw new IllegalStateException("Unhandled domain " + domain);
        }
    }

    public int size()
    {
        return items.length;
    }

    public void forEachWithKey(Seekable key, Consumer<T> consumer)
    {
        switch (key.domain())
        {
            case Key:
                for (int i = firstPossibleKeyIdx((PartitionKey) key); i < items.length; i++)
                {
                    Keys keys = (Keys)getKeys(items[i]);
                    checkState(keys.size() == 1);
                    if (keys.get(0).equals(key))
                        consumer.accept(items[i]);
                    else
                        break;
                }
                break;
            case Range:
                TokenRange range = (TokenRange) key;
                for (int i = firstPossibleRangeIdx(range); i < items.length; i++)
                {
                    Ranges ranges = (Ranges) getKeys(items[i]);
                    if (ranges.intersects(range))
                        consumer.accept(items[i]);
                    else
                        break;
                }
                break;
            default:
                throw new IllegalStateException("Unhandled domain " + key.domain());
        }
    }

    private int firstPossibleRangeIdx(TokenRange range)
    {
        int idx = Arrays.binarySearch(items, range, (l, r) -> {
            Ranges ranges = (Ranges)getKeys((T) l);
            if (ranges.intersects((TokenRange)r))
                return 1;
            if (((TokenRange) r).end().compareTo(ranges.get(ranges.size() - 1).end()) > 0)
                return 1;
            else
                return -1;
        });

        return -1 - idx;
    }

    private int firstPossibleKeyIdx(PartitionKey key)
    {
        int idx = Arrays.binarySearch(items, key, (l, r) -> {
            PartitionKey lk = (PartitionKey) getKeys((T) l).get(0);
            PartitionKey rk = (PartitionKey) r;
            int cmp = lk.compareTo(rk);
            return cmp != 0 ? cmp : 1;
        });

        return -1 - idx;
    }
}
