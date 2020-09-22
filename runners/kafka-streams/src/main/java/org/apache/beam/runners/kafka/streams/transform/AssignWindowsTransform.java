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

import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.joda.time.Instant;

public class AssignWindowsTransform<T> implements ValueMapper<WindowedValue<T>, WindowedValue<T>> {

  private final WindowFn<T, BoundedWindow> windowFn;

  public AssignWindowsTransform(WindowFn<T, BoundedWindow> windowFn) {
    this.windowFn = windowFn;
  }

  @Override
  public WindowedValue<T> apply(WindowedValue<T> windowedValue) {
    T element = windowedValue.getValue();
    Instant timestamp = windowedValue.getTimestamp();
    BoundedWindow boundedWindow = Iterables.getOnlyElement(windowedValue.getWindows());
    Collection<BoundedWindow> windows;
    try {
      windows =
          windowFn.assignWindows(
              windowFn.new AssignContext() {
                @Override
                public T element() {
                  return element;
                }

                @Override
                public Instant timestamp() {
                  return timestamp;
                }

                @Override
                public BoundedWindow window() {
                  return boundedWindow;
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return WindowedValue.of(element, timestamp, windows, windowedValue.getPane());
  }
}
