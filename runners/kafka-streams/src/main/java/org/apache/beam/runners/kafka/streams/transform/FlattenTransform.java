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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.Instant;

public class FlattenTransform<T>
    implements Transformer<
        Void, WatermarkOrWindowedValue<T>, KeyValue<Void, WatermarkOrWindowedValue<T>>> {

  private Map<Bytes, Instant> duplicates;

  @Override
  public void init(ProcessorContext context) {
    duplicates = new HashMap<>();
  }

  @Override
  public KeyValue<Void, WatermarkOrWindowedValue<T>> transform(
      Void key, WatermarkOrWindowedValue<T> watermarkOrWindowedValue) {
    if (watermarkOrWindowedValue.windowedValue() == null) {
      Instant watermark =
          duplicates.put(
              watermarkOrWindowedValue.watermark().id(),
              watermarkOrWindowedValue.watermark().watermark());
      if (watermarkOrWindowedValue.watermark().watermark().isEqual(watermark)) {
        return null;
      }
    }
    return KeyValue.pair(null, watermarkOrWindowedValue);
  }

  @Override
  public void close() {}
}
