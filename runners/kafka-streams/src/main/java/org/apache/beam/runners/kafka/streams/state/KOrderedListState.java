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
package org.apache.beam.runners.kafka.streams.state;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class KOrderedListState<K, V> extends KAbstractState<K, Collection<TimestampedValue<V>>>
    implements OrderedListState<V> {

  protected KOrderedListState(
      K key,
      StateNamespace namespace,
      String id,
      KeyValueStore<KV<K, String>, byte[]> store,
      Coder<V> coder) {
    super(
        key,
        namespace,
        id,
        store,
        CoderSerde.of(CollectionCoder.of(TimestampedValueCoder.of(coder))));
  }

  @Override
  public void add(TimestampedValue<V> value) {
    PriorityQueue<TimestampedValue<V>> queue =
        new PriorityQueue<>(new TimestampedValueCompator<>());
    Collection<TimestampedValue<V>> collection = get();
    if (collection != null) {
      queue.addAll(collection);
    }
    queue.add(value);
    set(queue);
  }

  @Override
  public void clear() {
    super.clear();
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return new ReadableState<Boolean>() {

      @Override
      public Boolean read() {
        return get() == null;
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }

  @Override
  public @Nullable Iterable<TimestampedValue<V>> read() {
    Collection<TimestampedValue<V>> collection = get();
    if (collection == null) {
      return Collections.emptyList();
    } else {
      return collection;
    }
  }

  @Override
  public GroupingState<TimestampedValue<V>, Iterable<TimestampedValue<V>>> readLater() {
    return this;
  }

  @Override
  public Iterable<TimestampedValue<V>> readRange(Instant minTimestamp, Instant limitTimestamp) {
    Collection<TimestampedValue<V>> collection = get();
    if (collection == null) {
      return Collections.emptyList();
    }
    return collection.stream()
        .filter(
            timestampedValue -> {
              Instant timestamp = timestampedValue.getTimestamp();
              boolean greaterThanOrEqualMinTimestamp =
                  timestamp.isAfter(minTimestamp) || timestamp.isEqual(minTimestamp);
              boolean lessThanLimitTimestamp = timestamp.isBefore(limitTimestamp);
              return greaterThanOrEqualMinTimestamp && lessThanLimitTimestamp;
            })
        .collect(Collectors.toList());
  }

  @Override
  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
    Collection<TimestampedValue<V>> collection = get();
    if (collection == null) {
      return;
    }
    for (TimestampedValue<V> timestampedValue : readRange(minTimestamp, limitTimestamp)) {
      collection.remove(timestampedValue);
    }
    if (collection.isEmpty()) {
      clear();
    } else {
      set(collection);
    }
  }

  @Override
  public OrderedListState<V> readRangeLater(Instant minTimestamp, Instant limitTimestamp) {
    return this;
  }

  private static class TimestampedValueCompator<T>
      implements Comparator<TimestampedValue<T>>, Serializable {

    @Override
    public int compare(TimestampedValue<T> left, TimestampedValue<T> right) {
      return left.getTimestamp().compareTo(right.getTimestamp());
    }
  }
}
