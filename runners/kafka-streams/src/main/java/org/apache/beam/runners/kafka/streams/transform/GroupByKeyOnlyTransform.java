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

import java.util.Collections;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class GroupByKeyOnlyTransform<K, V>
    implements KeyValueMapper<
        Void,
        WatermarkOrWindowedValue<KV<K, V>>,
        Iterable<KeyValue<K, WatermarkOrWindowedValue<V>>>> {

  @Override
  public Iterable<KeyValue<K, WatermarkOrWindowedValue<V>>> apply(
      Void key, WatermarkOrWindowedValue<KV<K, V>> watermarkOrWindowedValue) {
    if (watermarkOrWindowedValue.windowedValue() == null) {
      ImmutableList.Builder<KeyValue<K, WatermarkOrWindowedValue<V>>> builder =
          ImmutableList.builder();
      // TODO: For loop on number of partitions.
      for (int i = 0; i < 1; i++) {
        builder.add(KeyValue.pair(null, watermarkOrWindowedValue.toNewType()));
      }
      return builder.build();
    }
    return Collections.singleton(
        KeyValue.pair(
            watermarkOrWindowedValue.windowedValue().getValue().getKey(),
            WatermarkOrWindowedValue.of(
                watermarkOrWindowedValue
                    .windowedValue()
                    .withValue(watermarkOrWindowedValue.windowedValue().getValue().getValue()))));
  }
}
