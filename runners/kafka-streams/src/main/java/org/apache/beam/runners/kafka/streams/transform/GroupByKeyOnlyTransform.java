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
package org.apache.beam.runners.kafka.streams.transform;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class GroupByKeyOnlyTransform<K, V>
    implements KeyValueMapper<Void, WindowedValue<KV<K, V>>, KeyValue<K, WindowedValue<V>>> {

  @Override
  public KeyValue<K, WindowedValue<V>> apply(Void key, WindowedValue<KV<K, V>> windowedValue) {
    return KeyValue.pair(
        windowedValue.getValue().getKey(),
        windowedValue.withValue(windowedValue.getValue().getValue()));
  }
}
