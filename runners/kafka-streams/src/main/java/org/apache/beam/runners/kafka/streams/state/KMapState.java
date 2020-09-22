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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;

public class KMapState<K, KeyT, ValueT> extends KAbstractState<K, Map<KeyT, ValueT>>
    implements MapState<KeyT, ValueT> {

  protected KMapState(
      K key,
      StateNamespace namespace,
      String id,
      KeyValueStore<KV<K, String>, byte[]> store,
      Coder<KeyT> keyCoder,
      Coder<ValueT> valueCoder) {
    super(key, namespace, id, store, CoderSerde.of(MapCoder.of(keyCoder, valueCoder)));
  }

  @Override
  public void clear() {
    super.clear();
  }

  @Override
  public ReadableState<Iterable<Entry<KeyT, ValueT>>> entries() {
    return new ReadableState<Iterable<Entry<KeyT, ValueT>>>() {

      @Override
      public Iterable<Entry<KeyT, ValueT>> read() {
        Map<KeyT, ValueT> map = get();
        if (map == null) {
          return Collections.emptySet();
        } else {
          return map.entrySet();
        }
      }

      @Override
      public ReadableState<Iterable<Entry<KeyT, ValueT>>> readLater() {
        return this;
      }
    };
  }

  @Override
  public ReadableState<ValueT> get(KeyT key) {
    return new ReadableState<ValueT>() {

      @Override
      public ValueT read() {
        Map<KeyT, ValueT> map = get();
        if (map == null) {
          return null;
        } else {
          return map.get(key);
        }
      }

      @Override
      public ReadableState<ValueT> readLater() {
        return this;
      }
    };
  }

  @Override
  public ReadableState<Iterable<KeyT>> keys() {
    return new ReadableState<Iterable<KeyT>>() {

      @Override
      public Iterable<KeyT> read() {
        Map<KeyT, ValueT> map = get();
        if (map == null) {
          return Collections.emptySet();
        } else {
          return map.keySet();
        }
      }

      @Override
      public ReadableState<Iterable<KeyT>> readLater() {
        return this;
      }
    };
  }

  @Override
  public void put(KeyT key, ValueT value) {
    Map<KeyT, ValueT> map = get();
    if (map == null) {
      map = new HashMap<>();
    }
    map.put(key, value);
    set(map);
  }

  @Override
  public ReadableState<ValueT> putIfAbsent(KeyT key, ValueT value) {
    return new ReadableState<ValueT>() {

      @Override
      public ValueT read() {
        Map<KeyT, ValueT> map = get();
        if (map == null) {
          map = new HashMap<>();
        }
        if (map.containsKey(key)) {
          return map.get(key);
        } else {
          map.put(key, value);
          set(map);
          return null;
        }
      }

      @Override
      public ReadableState<ValueT> readLater() {
        return this;
      }
    };
  }

  @Override
  public void remove(KeyT key) {
    Map<KeyT, ValueT> map = get();
    if (map != null && map.containsKey(key)) {
      map.remove(key);
      if (map.isEmpty()) {
        super.clear();
      } else {
        set(map);
      }
    }
  }

  @Override
  public ReadableState<Iterable<ValueT>> values() {
    return new ReadableState<Iterable<ValueT>>() {

      @Override
      public Iterable<ValueT> read() {
        Map<KeyT, ValueT> map = get();
        if (map == null) {
          return Collections.emptyList();
        } else {
          return map.values();
        }
      }

      @Override
      public ReadableState<Iterable<ValueT>> readLater() {
        return this;
      }
    };
  }
}
