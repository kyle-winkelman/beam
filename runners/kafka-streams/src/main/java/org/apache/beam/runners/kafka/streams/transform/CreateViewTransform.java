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

import java.util.stream.Collectors;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class CreateViewTransform<T>
    implements KeyValueMapper<Void, WindowedValue<T>, Iterable<KeyValue<String, T>>> {

  private final Coder<BoundedWindow> windowCoder;

  public CreateViewTransform(Coder<BoundedWindow> windowCoder) {
    this.windowCoder = windowCoder;
  }

  @Override
  public Iterable<KeyValue<String, T>> apply(Void key, WindowedValue<T> windowedValue) {
    return windowedValue.getWindows().stream()
        .map(
            window ->
                KeyValue.pair(
                    StateNamespaces.window(windowCoder, window).stringKey(),
                    windowedValue.getValue()))
        .collect(Collectors.toList());
  }
}
