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
import java.util.stream.Collectors;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class CreateViewTransform<T>
    implements KeyValueMapper<
        Void, WatermarkOrWindowedValue<T>, Iterable<KeyValue<BoundedWindow, T>>> {

  public CreateViewTransform() {}

  @Override
  public Iterable<KeyValue<BoundedWindow, T>> apply(
      Void key, WatermarkOrWindowedValue<T> watermarkOrWindowedValue) {
    if (watermarkOrWindowedValue.windowedValue() == null) {
      return Collections.emptyList();
    }
    return watermarkOrWindowedValue.windowedValue().getWindows().stream()
        .map(
            window ->
                KeyValue.pair(
                    (BoundedWindow) window, watermarkOrWindowedValue.windowedValue().getValue()))
        .collect(Collectors.toList());
  }
}
