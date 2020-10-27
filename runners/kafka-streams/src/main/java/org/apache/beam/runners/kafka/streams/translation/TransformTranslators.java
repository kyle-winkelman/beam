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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessElements;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.runners.kafka.streams.override.KafkaStreamsCreateViewPTransformOverrideFactory;
import org.apache.beam.runners.kafka.streams.override.KafkaStreamsCreateViewPTransformOverrideFactory.KafkaStreamsCreateView;
import org.apache.beam.runners.kafka.streams.transform.AssignWindowsTransform;
import org.apache.beam.runners.kafka.streams.transform.CreateViewTransform;
import org.apache.beam.runners.kafka.streams.transform.FlattenTransform;
import org.apache.beam.runners.kafka.streams.transform.GroupAlsoByWindowTransform;
import org.apache.beam.runners.kafka.streams.transform.GroupByKeyOnlyTransform;
import org.apache.beam.runners.kafka.streams.transform.ParDoTransform;
import org.apache.beam.runners.kafka.streams.transform.SplittableGBKIKWITransform;
import org.apache.beam.runners.kafka.streams.transform.SplittableProcessTransform;
import org.apache.beam.runners.kafka.streams.transform.StatefulParDoTransform;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public class TransformTranslators {
  private static final Map<String, TransformTranslator<?, ?, ?>> TRANSFORM_TRANSLATORS;

  static {
    TRANSFORM_TRANSLATORS = new HashMap<>();
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
        new AssignWindowsTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        KafkaStreamsCreateViewPTransformOverrideFactory.KAFKA_STREAMS_CREATE_VIEW_URN,
        new CreateViewTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTransformTranslator());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        SplittableParDo.SPLITTABLE_GBKIKWI_URN, new SplittableGBKIKWITransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        SplittableParDo.SPLITTABLE_PROCESS_URN, new SplittableProcessTransformTranslator<>());
  }

  public static boolean has(String urn) {
    return TRANSFORM_TRANSLATORS.containsKey(urn);
  }

  public static TransformTranslator<?, ?, ?> get(String urn) {
    return TRANSFORM_TRANSLATORS.get(urn);
  }

  private static class AssignWindowsTransformTranslator<T>
      extends TransformTranslator<PCollection<T>, PCollection<T>, Window<T>> {

    @Override
    void translate(
        AppliedPTransform<PCollection<T>, PCollection<T>, Window<T>> appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      String named = named(appliedPTransform);
      PCollection<T> output = output(appliedPTransform);
      WindowFn<T, BoundedWindow> windowFn = windowingStrategy(output).getWindowFn();
      KStream<Void, WatermarkOrWindowedValue<T>> window =
          context
              .getStream(mainInput(appliedPTransform))
              .mapValues(new AssignWindowsTransform<>(windowFn), Named.as(named + "_MapValues"));
      context.putStream(output, window);
    }
  }

  private static class CreateViewTransformTranslator<ElemT, ViewT>
      extends TransformTranslator<
          PCollection<List<ElemT>>,
          PCollection<List<ElemT>>,
          KafkaStreamsCreateView<ElemT, ViewT>> {

    @Override
    void translate(
        AppliedPTransform<
                PCollection<List<ElemT>>,
                PCollection<List<ElemT>>,
                KafkaStreamsCreateView<ElemT, ViewT>>
            appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      String named = named(appliedPTransform);
      PCollection<List<ElemT>> input = mainInput(appliedPTransform);
      Serde<BoundedWindow> keySerde =
          CoderSerde.of(windowingStrategy(input).getWindowFn().windowCoder());
      Serde<List<ElemT>> valueSerde = CoderSerde.of(input.getCoder());
      context
          .getStream(mainInput(appliedPTransform))
          .flatMap(new CreateViewTransform<>())
          .to(named, Produced.with(keySerde, valueSerde).withName(named));
      context.putView(appliedPTransform.getTransform().getView(), named, keySerde, valueSerde);
    }
  }

  private static class FlattenTransformTranslator<T>
      extends TransformTranslator<PCollectionList<T>, PCollection<T>, Flatten.PCollections<T>> {

    @Override
    void translate(
        AppliedPTransform<PCollectionList<T>, PCollection<T>, PCollections<T>> appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      String named = named(appliedPTransform);
      List<PCollection<T>> mainInputs = mainInputs(appliedPTransform);
      KStream<Void, WatermarkOrWindowedValue<T>> flatten = null;
      for (int i = 0; i < mainInputs.size(); i++) {
        if (i == 0) {
          flatten = context.getStream(mainInputs.get(i));
        } else {
          flatten =
              flatten.merge(context.getStream(mainInputs.get(i)), Named.as(named + "_Merge_" + i));
        }
      }
      if (mainInputs.size() > 1) {
        flatten = flatten.transform(() -> new FlattenTransform<>());
      }
      context.putStream(output(appliedPTransform), flatten);
    }
  }

  private static class GroupByKeyTransformTranslator<K, V>
      extends TransformTranslator<
          PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

    @Override
    void translate(
        AppliedPTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>>
            appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      String named = named(appliedPTransform);
      String stateStoreName = named + "_GroupAlsoByWindow_Transform_State";
      String timerStoreName = named + "_GroupAlsoByWindow_Transform_Timer";
      PCollection<KV<K, V>> input = mainInput(appliedPTransform);
      KvCoder<K, V> kvCoder = kvCoder(input);
      Serde<K> keySerde = CoderSerde.of(NullableCoder.of(kvCoder.getKeyCoder()));
      Coder<V> valueCoder = kvCoder.getValueCoder();
      Serde<WatermarkOrWindowedValue<V>> watermarkOrWindowedValueSerde =
          CoderSerde.of(coder(input, valueCoder));
      KStream<K, WatermarkOrWindowedValue<V>> groupByKeyOnly =
          context
              .getStream(input)
              .flatMap(new GroupByKeyOnlyTransform<>(), Named.as(named + "_GroupByKeyOnly_Map"))
              .through(
                  named,
                  Produced.with(keySerde, watermarkOrWindowedValueSerde)
                      .withName(named + "_GroupByKeyOnly_Through"));
      context.addStateStore(stateStoreName, kvCoder.getKeyCoder());
      context.addTimerStore(timerStoreName, kvCoder.getKeyCoder(), windowCoder(input));
      KStream<Void, WatermarkOrWindowedValue<KV<K, Iterable<V>>>> groupAlsoByWindow =
          groupByKeyOnly.transform(
              () ->
                  new GroupAlsoByWindowTransform<>(
                      named,
                      input.getPipeline().getOptions(),
                      windowingStrategy(input),
                      valueCoder,
                      stateStoreName,
                      timerStoreName),
              Named.as(named + "_GroupAlsoByWindow_Transform"),
              stateStoreName,
              timerStoreName);
      context.putStream(output(appliedPTransform), groupAlsoByWindow);
    }
  }

  private static class ImpulseTransformTranslator
      extends TransformTranslator<PBegin, PCollection<byte[]>, Impulse> {

    @Override
    void translate(
        AppliedPTransform<PBegin, PCollection<byte[]>, Impulse> appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      context.putStream(output(appliedPTransform), context.impulse());
    }
  }

  private static class SplittableGBKIKWITransformTranslator<V>
      extends TransformTranslator<
          PCollection<KV<byte[], V>>,
          PCollection<KeyedWorkItem<byte[], V>>,
          GBKIntoKeyedWorkItems<V>> {

    @Override
    void translate(
        AppliedPTransform<
                PCollection<KV<byte[], V>>,
                PCollection<KeyedWorkItem<byte[], V>>,
                GBKIntoKeyedWorkItems<V>>
            appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      String named = named(appliedPTransform);
      PCollection<KV<byte[], V>> input = mainInput(appliedPTransform);
      KvCoder<byte[], V> kvCoder = kvCoder(input);
      Serde<byte[]> keySerde = CoderSerde.of(NullableCoder.of(kvCoder.getKeyCoder()));
      Coder<V> valueCoder = kvCoder.getValueCoder();
      Serde<WatermarkOrWindowedValue<V>> windowedValueSerde =
          CoderSerde.of(coder(input, valueCoder));
      KStream<Void, WatermarkOrWindowedValue<KeyedWorkItem<byte[], V>>> gbkikwi =
          context
              .getStream(input)
              .flatMap(new GroupByKeyOnlyTransform<>(), Named.as(named + "_GroupByKeyOnly_Map"))
              .through(
                  named,
                  Produced.with(keySerde, windowedValueSerde)
                      .withName(named + "_GroupByKeyOnly_Through"))
              .map(
                  new SplittableGBKIKWITransform<>(),
                  Named.as(named + "_GroupByKeyIntoKeyedWorkItems_Map"));
      context.putStream(output(appliedPTransform), gbkikwi);
    }
  }

  private static class SplittableProcessTransformTranslator<
          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      extends TransformTranslator<
          PCollection<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>>,
          PCollectionTuple,
          ProcessElements<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>> {

    @Override
    void translate(
        AppliedPTransform<
                PCollection<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>>,
                PCollectionTuple,
                ProcessElements<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>>
            appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      String named = named(appliedPTransform);
      String stateStoreName = named + "_Transform_State";
      String timerStoreName = named + "_Transform_Timer";
      PCollection<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> input =
          mainInput(appliedPTransform);
      context.addStateStore(stateStoreName, ByteArrayCoder.of());
      context.addTimerStore(timerStoreName, ByteArrayCoder.of(), windowCoder(input));
      KStream<TupleTag<?>, WatermarkOrWindowedValue<?>> process =
          context
              .getStream(input)
              .transform(
                  () ->
                      new SplittableProcessTransform<>(
                          named,
                          appliedPTransform.getPipeline().getOptions(),
                          appliedPTransform
                              .getTransform()
                              .newProcessFn(appliedPTransform.getTransform().getFn()),
                          context.getViews(appliedPTransform.getTransform().getSideInputs()),
                          appliedPTransform.getTransform().getMainOutputTag(),
                          appliedPTransform.getTransform().getAdditionalOutputTags().getAll(),
                          input.getCoder(),
                          outputCoders(appliedPTransform),
                          windowingStrategy(input),
                          DoFnSchemaInformation.create(),
                          sideInputMapping(appliedPTransform.getTransform().getSideInputs()),
                          stateStoreName,
                          timerStoreName,
                          appliedPTransform.getTransform().getFn()),
                  Named.as(named + "_Transform"),
                  stateStoreName,
                  timerStoreName);
      context.putMultiStream(named, outputs(appliedPTransform), process);
    }
  }

  private static class ParDoTransformTranslator<InputT, OutputT, K, V>
      extends TransformTranslator<
          PCollection<InputT>,
          PCollectionTuple,
          PTransform<PCollection<InputT>, PCollectionTuple>> {

    @Override
    void translate(
        AppliedPTransform<
                PCollection<InputT>,
                PCollectionTuple,
                PTransform<PCollection<InputT>, PCollectionTuple>>
            appliedPTransform,
        AppliedPTransform<?, ?, ?> enclosingAppliedPTransform,
        TranslationContext context) {
      String named = named(appliedPTransform);
      PCollection<InputT> input = mainInput(appliedPTransform);
      KStream<Void, WatermarkOrWindowedValue<InputT>> stream = context.getStream(input);
      KStream<TupleTag<?>, WatermarkOrWindowedValue<?>> parDo;
      if (usesStateOrTimers(appliedPTransform)) {
        String stateStoreName = named + "_Transform_State";
        String timerStoreName = named + "_Transform_Timer";
        KvCoder<K, V> kvCoder = kvCoder(input);
        context.addStateStore(stateStoreName, kvCoder.getKeyCoder());
        context.addTimerStore(timerStoreName, kvCoder.getKeyCoder(), windowCoder(input));
        parDo =
            stream.transform(
                () ->
                    new StatefulParDoTransform<>(
                        named,
                        appliedPTransform.getPipeline().getOptions(),
                        doFn(appliedPTransform),
                        context.getViews(sideInputMapping(appliedPTransform)),
                        mainOutputTag(appliedPTransform),
                        additionalOutputTags(appliedPTransform),
                        input.getCoder(),
                        outputCoders(appliedPTransform),
                        windowingStrategy(input),
                        doFnSchemaInformation(appliedPTransform),
                        sideInputMapping(appliedPTransform),
                        stateStoreName,
                        timerStoreName),
                Named.as(named + "_Transform"),
                stateStoreName,
                timerStoreName);
      } else {
        parDo =
            stream.transform(
                () ->
                    new ParDoTransform<>(
                        named,
                        appliedPTransform.getPipeline().getOptions(),
                        doFn(appliedPTransform),
                        context.getViews(sideInputMapping(appliedPTransform)),
                        mainOutputTag(appliedPTransform),
                        additionalOutputTags(appliedPTransform),
                        input.getCoder(),
                        outputCoders(appliedPTransform),
                        windowingStrategy(input),
                        doFnSchemaInformation(appliedPTransform),
                        sideInputMapping(appliedPTransform)),
                Named.as(named + "_Transform"));
      }
      context.putMultiStream(named, outputs(appliedPTransform), parDo);
    }
  }
}
