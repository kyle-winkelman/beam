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
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;

public class KSetState<K, V> extends KAbstractState<K, Set<V>> implements SetState<V> {

  protected KSetState(
      K key,
      StateNamespace namespace,
      String id,
      KeyValueStore<KV<K, String>, byte[]> store,
      Coder<V> elemCoder) {
    super(key, namespace, id, store, CoderSerde.of(SetCoder.of(elemCoder)));
  }

  @Override
  public void add(V value) {
    Set<V> set = get();
    if (set == null) {
      set = new HashSet<>();
    }
    set.add(value);
    set(set);
  }

  @Override
  public ReadableState<Boolean> addIfAbsent(V value) {
    return new ReadableState<Boolean>() {

      @Override
      public Boolean read() {
        Set<V> set = get();
        if (set == null) {
          set = new HashSet<>();
        }
        if (set.contains(value)) {
          return false;
        } else {
          set.add(value);
          set(set);
          return true;
        }
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }

  @Override
  public void clear() {
    super.clear();
  }

  @Override
  public ReadableState<Boolean> contains(V value) {
    return new ReadableState<Boolean>() {

      @Override
      public Boolean read() {
        Set<V> set = get();
        return set != null && set.contains(value);
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
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
  public Iterable<V> read() {
    Set<V> set = get();
    if (set == null) {
      return Collections.emptySet();
    } else {
      return set;
    }
  }

  @Override
  public SetState<V> readLater() {
    return this;
  }

  @Override
  public void remove(V t) {
    Set<V> set = get();
    if (set != null) {
      if (set.remove(t)) {
        set(set);
      }
    }
  }
}
