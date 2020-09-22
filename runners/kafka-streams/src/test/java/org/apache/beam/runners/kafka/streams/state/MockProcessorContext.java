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

import java.io.File;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;

/**
 * {@link ProcessorContext} that implements the {@link ProcessorContext#getStateStore(String)}
 * method and is backed by a {@link Map} of {@link StateStore StateStores}.
 */
public class MockProcessorContext implements ProcessorContext {

  private final Map<String, StateStore> stateStores;

  public MockProcessorContext(Map<String, StateStore> stateStores) {
    this.stateStores = stateStores;
  }

  @Override
  public StateStore getStateStore(String name) {
    return stateStores.get(name);
  }

  @Override
  public Map<String, Object> appConfigs() {
    return null;
  }

  @Override
  public Map<String, Object> appConfigsWithPrefix(String prefix) {
    return null;
  }

  @Override
  public String applicationId() {
    return null;
  }

  @Override
  public void commit() {}

  @Override
  public Headers headers() {
    return null;
  }

  @Override
  public <K, V> void forward(K key, V value) {}

  @Override
  public <K, V> void forward(K key, V value, int childIndex) {}

  @Override
  public <K, V> void forward(K key, V value, String childName) {}

  @Override
  public <K, V> void forward(K key, V value, To to) {}

  @Override
  public Serde<?> keySerde() {
    return null;
  }

  @Override
  public StreamsMetrics metrics() {
    return null;
  }

  @Override
  public long offset() {
    return 0;
  }

  @Override
  public int partition() {
    return 0;
  }

  @Override
  public void register(StateStore store, StateRestoreCallback stateRestoreCallback) {}

  @Override
  public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
    return null;
  }

  @Override
  public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
    return null;
  }

  @Override
  public File stateDir() {
    return null;
  }

  @Override
  public TaskId taskId() {
    return null;
  }

  @Override
  public long timestamp() {
    return 0;
  }

  @Override
  public String topic() {
    return null;
  }

  @Override
  public Serde<?> valueSerde() {
    return null;
  }
}
