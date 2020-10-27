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
package org.apache.beam.runners.kafka.streams.translation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoderV2;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.runners.kafka.streams.coder.WatermarkOrWindowedValueCoder;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.Stores;

public class TranslationContext {

  private static final String IMPULSE_TOPIC_SUFFIX = "-impulse";

  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final StreamsBuilder streamsBuilder;
  private final Map<PCollection<?>, KStream<?, ?>> streams;
  private final Map<PCollectionView<?>, String> views;
  private final String impulseTopic;
  private final KStream<Void, WatermarkOrWindowedValue<byte[]>> impulse;

  public TranslationContext(KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    this.streamsBuilder = new StreamsBuilder();
    this.streams = new HashMap<>();
    this.views = new HashMap<>();
    this.impulseTopic = applicationId() + IMPULSE_TOPIC_SUFFIX;
    this.impulse =
        streamsBuilder.stream(
            impulseTopic,
            Consumed.with(
                    CoderSerde.of(VoidCoder.of()),
                    CoderSerde.of(
                        WatermarkOrWindowedValueCoder.of(
                            WindowedValue.getFullCoder(
                                ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE))))
                .withName("Impulse"));
  }

  public StreamsBuilder getStreamsBuilder() {
    return streamsBuilder;
  }

  public String getImpulseTopic() {
    return impulseTopic;
  }

  @SuppressWarnings("unchecked")
  <T> KStream<Void, WatermarkOrWindowedValue<T>> getStream(PCollection<T> pCollection) {
    return (KStream<Void, WatermarkOrWindowedValue<T>>) streams.get(pCollection);
  }

  <T> void putStream(
      PCollection<T> pCollection, KStream<Void, WatermarkOrWindowedValue<T>> stream) {
    streams.put(pCollection, stream);
  }

  void putMultiStream(
      String named,
      Map<TupleTag<?>, PCollection<?>> outputs,
      KStream<TupleTag<?>, WatermarkOrWindowedValue<?>> stream) {
    List<Map.Entry<TupleTag<?>, PCollection<?>>> outputList = new ArrayList<>(outputs.entrySet());
    Predicate<TupleTag<?>, WatermarkOrWindowedValue<?>>[] predicates =
        new Predicate[outputList.size()];
    for (int i = 0; i < outputList.size(); i++) {
      predicates[i] = new TupleTagPredicate<>(outputList.get(i).getKey());
    }
    KStream<TupleTag<?>, WatermarkOrWindowedValue<?>>[] branches =
        stream.branch(Named.as(named + "_Branch"), predicates);
    for (int i = 0; i < outputList.size(); i++) {
      KStream<Void, WatermarkOrWindowedValue<?>> branch =
          branches[i].map(
              (key, value) -> KeyValue.pair(null, value), Named.as(named + "_Branch_Map_" + i));
      streams.put(outputList.get(i).getValue(), branch);
    }
  }

  <K> void addStateStore(String stateStoreName, Coder<K> keyCoder) {
    getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(stateStoreName),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(NullableCoder.of(ByteArrayCoder.of()))));
  }

  <K> void addTimerStore(
      String timerStoreName, Coder<K> keyCoder, Coder<BoundedWindow> windowCoder) {
    getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(timerStoreName),
                CoderSerde.of(NullableCoder.of(keyCoder)),
                CoderSerde.of(
                    MapCoder.of(StringUtf8Coder.of(), TimerDataCoderV2.of(windowCoder)))));
  }

  <K, V> void putView(
      PCollectionView<?> pCollectionView, String named, Serde<K> keySerde, Serde<V> valueSerde) {
    getStreamsBuilder()
        .globalTable(
            named, Consumed.with(keySerde, valueSerde).withName(named), Materialized.as(named));
    views.put(pCollectionView, named);
  }

  Map<PCollectionView<?>, String> getViews(List<PCollectionView<?>> pCollectionViews) {
    return pCollectionViews.stream()
        .collect(Collectors.toMap(pCollectionView -> pCollectionView, views::get));
  }

  Map<PCollectionView<?>, String> getViews(Map<String, PCollectionView<?>> sideInputMapping) {
    return sideInputMapping.values().stream()
        .collect(Collectors.toMap(pCollectionView -> pCollectionView, views::get));
  }

  KStream<Void, WatermarkOrWindowedValue<byte[]>> impulse() {
    return impulse;
  }

  String applicationId() {
    return pipelineOptions.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG);
  }

  private static class TupleTagPredicate<T> implements Predicate<TupleTag<?>, T> {

    private final TupleTag<?> tupleTag;

    private TupleTagPredicate(TupleTag<?> tupleTag) {
      this.tupleTag = tupleTag;
    }

    @Override
    public boolean test(TupleTag<?> key, T value) {
      return Objects.equals(tupleTag, key);
    }
  }
}
