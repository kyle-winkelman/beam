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
package org.apache.beam.runners.kafka.streams.sideinput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class KSideInputReader implements ReadyCheckingSideInputReader {

  public static KSideInputReader of(
      ProcessorContext processorContext, Map<PCollectionView<?>, String> sideInputs) {
    return new KSideInputReader(processorContext, sideInputs);
  }

  private final ProcessorContext processorContext;
  private final Map<PCollectionView<?>, String> sideInputs;

  private KSideInputReader(
      ProcessorContext processorContext, Map<PCollectionView<?>, String> sideInputs) {
    this.processorContext = processorContext;
    this.sideInputs = sideInputs;
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view);
  }

  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    String namespace = namespace(view, window);
    switch (view.getViewFn().getMaterialization().getUrn()) {
      case Materializations.ITERABLE_MATERIALIZATION_URN:
        return ((ViewFn<IterableView<Object>, T>) view.getViewFn())
            .apply(
                new IterableMaterializationView<>(
                    ((ReadOnlyKeyValueStore<String, ValueAndTimestamp<List<Object>>>)
                            processorContext.getStateStore(sideInputs.get(view)))
                        .get(namespace)));
      case Materializations.MULTIMAP_MATERIALIZATION_URN:
        return ((ViewFn<MultimapView<Object, Object>, T>) view.getViewFn())
            .apply(
                new MultimapMaterializationView<>(
                    ((ReadOnlyKeyValueStore<String, ValueAndTimestamp<List<KV<Object, Object>>>>)
                            processorContext.getStateStore(sideInputs.get(view)))
                        .get(namespace)));
      default:
        throw new IllegalArgumentException(
            "Unknown materialization: " + view.getViewFn().getMaterialization().getUrn());
    }
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  @Override
  public boolean isReady(PCollectionView<?> view, BoundedWindow window) {
    String namespace = namespace(view, window);
    return ((ReadOnlyKeyValueStore<String, ValueAndTimestamp<List<Object>>>)
                processorContext.getStateStore(sideInputs.get(view)))
            .get(namespace)
        != null;
  }

  private String namespace(PCollectionView<?> view, BoundedWindow window) {
    return StateNamespaces.window(
            (Coder<BoundedWindow>) view.getWindowingStrategyInternal().getWindowFn().windowCoder(),
            view.getWindowMappingFn().getSideInputWindow(window))
        .stringKey();
  }

  private static class IterableMaterializationView<V> implements IterableView<V> {

    private final Iterable<V> iterable;

    private IterableMaterializationView(ValueAndTimestamp<List<V>> valueAndTimestamp) {
      if (valueAndTimestamp == null) {
        this.iterable = Collections.emptyList();
      } else {
        this.iterable = valueAndTimestamp.value();
      }
    }

    @Override
    public Iterable<V> get() {
      return iterable;
    }
  }

  private static class MultimapMaterializationView<K, V> implements MultimapView<K, V> {

    private final Map<K, Collection<V>> map;

    private MultimapMaterializationView(ValueAndTimestamp<List<KV<K, V>>> valueAndTimestamp) {
      if (valueAndTimestamp == null) {
        this.map = Collections.emptyMap();
      } else {
        this.map = new HashMap<>();
        for (KV<K, V> kv : valueAndTimestamp.value()) {
          Collection<V> collection = map.get(kv.getKey());
          if (collection == null) {
            collection = new ArrayList<>();
          }
          collection.add(kv.getValue());
          map.put(kv.getKey(), collection);
        }
      }
    }

    @Override
    public Iterable<V> get(K k) {
      Collection<V> collection = map.get(k);
      if (collection == null) {
        return Collections.emptyList();
      }
      return collection;
    }

    @Override
    public Iterable<K> get() {
      return map.keySet();
    }
  }
}
