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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/** {@link KeyValueStore} backed by a {@link Map}. */
public class MockKeyValueStore<K, V> implements KeyValueStore<K, V> {

  protected final Map<K, V> map;

  public MockKeyValueStore() {
    this.map = new HashMap<>();
  }

  @Override
  public V get(K key) {
    return map.get(key);
  }

  @Override
  public V delete(K key) {
    return map.remove(key);
  }

  @Override
  public void put(K key, V value) {
    map.put(key, value);
  }

  @Override
  public KeyValueIterator<K, V> all() {
    return new KeyValueIterator<K, V>() {

      private final Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();

      private KeyValue<K, V> peek;

      @Override
      public boolean hasNext() {
        if (peek != null) {
          return true;
        }
        return iterator.hasNext();
      }

      @Override
      public KeyValue<K, V> next() {
        if (peek != null) {
          KeyValue<K, V> next = peek;
          peek = null;
          return next;
        }
        Map.Entry<K, V> next = iterator.next();
        return KeyValue.pair(next.getKey(), next.getValue());
      }

      @Override
      public void close() {}

      @Override
      public K peekNextKey() {
        if (peek == null) {
          peek = next();
        }
        return peek.key;
      }
    };
  }

  @Override
  public long approximateNumEntries() {
    return 0;
  }

  @Override
  public void close() {}

  @Override
  public void flush() {}

  @Override
  public void init(ProcessorContext context, StateStore root) {}

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public boolean persistent() {
    return false;
  }

  @Override
  public KeyValueIterator<K, V> range(K from, K to) {
    return null;
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return null;
  }

  @Override
  public void putAll(List<KeyValue<K, V>> entries) {}
}
