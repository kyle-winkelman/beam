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
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;

public class KValueState<K, V> extends KAbstractState<K, V> implements ValueState<V> {

  protected KValueState(
      K key,
      StateNamespace namespace,
      String id,
      KeyValueStore<KV<K, String>, byte[]> store,
      Coder<V> coder) {
    super(key, namespace, id, store, CoderSerde.of(coder));
  }

  @Override
  public void clear() {
    super.clear();
  }

  @Override
  public V read() {
    return get();
  }

  @Override
  public KValueState<K, V> readLater() {
    return this;
  }

  @Override
  public void write(V input) {
    set(input);
  }
}
