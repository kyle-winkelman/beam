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
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoderV2;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.Instant;

public class TranslationContext {

  private static final String IMPULSE_TOPIC_SUFFIX = "-impulse";
  private static final String WATERMARK_TOPIC_SUFFIX = "-watermark";

  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final StreamsBuilder streamsBuilder;
  private final Map<PCollection<?>, KStream<?, ?>> streams;
  private final Map<PCollection<?>, Set<String>> streamSources;
  private final Map<PCollectionView<?>, String> views;
  private final String impulseTopic;
  private final KStream<Void, WindowedValue<byte[]>> impulse;
  private final String watermarkTopic;
  private final WatermarkConsumer watermarkConsumer;

  public TranslationContext(KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    this.streamsBuilder = new StreamsBuilder();
    this.streams = new HashMap<>();
    this.streamSources = new HashMap<>();
    this.views = new HashMap<>();
    this.impulseTopic = applicationId() + IMPULSE_TOPIC_SUFFIX;
    this.impulse =
        streamsBuilder.stream(
            impulseTopic,
            Consumed.with(
                    CoderSerde.of(VoidCoder.of()),
                    CoderSerde.of(
                        WindowedValue.getFullCoder(
                            ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE)))
                .withName("Impulse"));
    this.watermarkTopic = applicationId() + WATERMARK_TOPIC_SUFFIX;
    this.watermarkConsumer = new WatermarkConsumer();
    Serde<String> keySerde = CoderSerde.of(StringUtf8Coder.of());
    Serde<Map<byte[], Instant>> valueSerde =
        CoderSerde.of(MapCoder.of(ByteArrayCoder.of(), InstantCoder.of()));
    streamsBuilder.stream(
            watermarkTopic,
            Consumed.with(
                    keySerde, CoderSerde.of(KvCoder.of(ByteArrayCoder.of(), InstantCoder.of())))
                .withName("watermark"))
        .groupByKey()
        .<Map<byte[], Instant>>aggregate(
            HashMap::new,
            (key, value, aggregate) -> {
              aggregate.put(value.getKey(), value.getValue());
              return aggregate;
            },
            Materialized.with(keySerde, valueSerde))
        .toStream()
        .repartition(Repartitioned.with(keySerde, valueSerde).withName("watermark"));
    streamsBuilder.globalTable(
        applicationId() + "-watermark-repartition",
        Consumed.with(keySerde, valueSerde).withName("watermark-global"),
        Materialized.as("watermark"));
  }

  public StreamsBuilder getStreamsBuilder() {
    return streamsBuilder;
  }

  public String getImpulseTopic() {
    return impulseTopic;
  }

  public String getWatermarkTopic() {
    return watermarkTopic;
  }

  public WatermarkConsumer getWatermarkConsumer() {
    return watermarkConsumer;
  }

  @SuppressWarnings("unchecked")
  <T> KStream<Void, WindowedValue<T>> getStream(PCollection<T> pCollection) {
    return (KStream<Void, WindowedValue<T>>) streams.get(pCollection);
  }

  <T> void putStream(PCollection<T> pCollection, KStream<Void, WindowedValue<T>> stream) {
    streams.put(pCollection, stream);
  }

  void putMultiStream(
      String named,
      Map<TupleTag<?>, PCollection<?>> outputs,
      KStream<TupleTag<?>, WindowedValue<?>> stream) {
    List<Map.Entry<TupleTag<?>, PCollection<?>>> outputList = new ArrayList<>(outputs.entrySet());
    Predicate<? super TupleTag<?>, ? super WindowedValue<?>>[] predicates =
        new Predicate[outputList.size()];
    for (int i = 0; i < outputList.size(); i++) {
      predicates[i] = new TupleTagPredicate(outputList.get(i).getKey());
    }
    KStream<TupleTag<?>, WindowedValue<?>>[] branches =
        stream.branch(Named.as(named + "_Branch"), predicates);
    for (int i = 0; i < outputList.size(); i++) {
      KStream<Void, WindowedValue<?>> branch =
          branches[i].map(
              (key, value) -> KeyValue.pair(null, value), Named.as(named + "_Branch_Map_" + i));
      streams.put(outputList.get(i).getValue(), branch);
    }
  }

  Set<String> getStreamSource(PCollection<?> pCollection) {
    return streamSources.get(pCollection);
  }

  void putStreamSource(PCollection<?> pCollection, Set<String> streamSource) {
    streamSources.put(pCollection, streamSource);
  }

  void putMultiStreamSource(Map<TupleTag<?>, PCollection<?>> outputs, Set<String> streamSource) {
    for (PCollection<?> pCollection : outputs.values()) {
      putStreamSource(pCollection, streamSource);
    }
  }

  <K> void addStateStore(String stateStoreName, Coder<K> keyCoder) {
    getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(stateStoreName),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                Serdes.ByteArray()));
  }

  <K> void addTimerStore(
      String timerStoreName, Coder<K> keyCoder, Coder<BoundedWindow> windowCoder) {
    getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(timerStoreName),
                CoderSerde.of(keyCoder),
                CoderSerde.of(
                    MapCoder.of(StringUtf8Coder.of(), TimerDataCoderV2.of(windowCoder)))));
  }

  <K, V> void putView(
      PCollectionView<?> pCollectionView, String named, Serde<K> keySerde, Serde<V> valueSerde) {
    getStreamsBuilder()
        .globalTable(
            applicationId() + "-" + named + "-repartition",
            Consumed.with(keySerde, valueSerde).withName(named),
            Materialized.as(named));
    views.put(pCollectionView, named);
  }

  Map<PCollectionView<?>, String> getViews(List<PCollectionView<?>> pCollectionViews) {
    return pCollectionViews.stream()
        .collect(
            Collectors.toMap(
                pCollectionView -> pCollectionView, pCollectionView -> views.get(pCollectionView)));
  }

  Map<PCollectionView<?>, String> getViews(Map<String, PCollectionView<?>> sideInputMapping) {
    return sideInputMapping.values().stream()
        .collect(
            Collectors.toMap(
                pCollectionView -> pCollectionView, pCollectionView -> views.get(pCollectionView)));
  }

  KStream<Void, WindowedValue<byte[]>> impulse() {
    return impulse;
  }

  String applicationId() {
    return pipelineOptions.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG);
  }

  private static class TupleTagPredicate implements Predicate<TupleTag<?>, WindowedValue<?>> {

    private final TupleTag<?> tupleTag;

    private TupleTagPredicate(TupleTag<?> tupleTag) {
      this.tupleTag = tupleTag;
    }

    @Override
    public boolean test(TupleTag<?> key, WindowedValue<?> value) {
      return tupleTag.equals(key);
    }
  }

  public static class WatermarkConsumer implements Consumer<KV<String, KV<byte[], Instant>>> {

    private Consumer<KV<String, KV<byte[], Instant>>> consumer;

    @Override
    public void accept(KV<String, KV<byte[], Instant>> t) {
      consumer.accept(t);
    }

    public void setConsumer(Consumer<KV<String, KV<byte[], Instant>>> consumer) {
      this.consumer = consumer;
    }
  }
}
