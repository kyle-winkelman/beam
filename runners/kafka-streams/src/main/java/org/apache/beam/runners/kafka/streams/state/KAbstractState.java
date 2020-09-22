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

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;

public abstract class KAbstractState<K, V> {

  private final KV<K, String> key;
  private final KeyValueStore<KV<K, String>, byte[]> store;
  private final Serde<V> serde;

  protected KAbstractState(
      K key,
      StateNamespace namespace,
      String id,
      KeyValueStore<KV<K, String>, byte[]> store,
      Serde<V> serde) {
    this.key = KV.of(key, namespace.stringKey() + "+" + id);
    this.store = store;
    this.serde = serde;
  }

  protected V get() {
    byte[] value = store.get(key);
    if (value == null) {
      return null;
    } else {
      return serde.deserializer().deserialize(null, value);
    }
  }

  protected void set(V value) {
    if (value == null) {
      store.put(key, null);
    } else {
      store.put(key, serde.serializer().serialize(null, value));
    }
  }

  protected void clear() {
    store.delete(key);
  }
}
