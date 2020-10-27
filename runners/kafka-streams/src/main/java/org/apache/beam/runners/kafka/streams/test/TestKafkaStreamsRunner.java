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
package org.apache.beam.runners.kafka.streams.test;

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.EmptyFlattenAsCreateFactory;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.TestStreamAsSplittableDoFnFactory;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.coder.BytesCoder;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.runners.kafka.streams.coder.WatermarkOrWindowedValueCoder;
import org.apache.beam.runners.kafka.streams.override.KafkaStreamsCreateViewPTransformOverrideFactory;
import org.apache.beam.runners.kafka.streams.state.KTimerInternals;
import org.apache.beam.runners.kafka.streams.translation.PipelineTranslator;
import org.apache.beam.runners.kafka.streams.translation.TranslationContext;
import org.apache.beam.runners.kafka.streams.watermark.Watermark;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

public class TestKafkaStreamsRunner extends PipelineRunner<TestKafkaStreamsPipelineResult> {

  public static TestKafkaStreamsRunner fromOptions(PipelineOptions pipelineOptions) {
    KafkaStreamsPipelineOptions kafkaStreamsPipelineOptions =
        pipelineOptions.as(KafkaStreamsPipelineOptions.class);
    kafkaStreamsPipelineOptions.setProperties(
        ImmutableMap.<String, String>builder()
            .put("bootstrap.servers", "")
            .put("application.id", UUID.randomUUID().toString())
            .build());
    return new TestKafkaStreamsRunner(kafkaStreamsPipelineOptions);
  }

  private final KafkaStreamsPipelineOptions pipelineOptions;

  private TestKafkaStreamsRunner(KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions.as(KafkaStreamsPipelineOptions.class);
  }

  @Override
  public TestKafkaStreamsPipelineResult run(Pipeline pipeline) {
    pipeline.replaceAll(
        ImmutableList.<PTransformOverride>builder()
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(PTransformTranslation.TEST_STREAM_TRANSFORM_URN),
                    new TestStreamAsSplittableDoFnFactory<>()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN),
                    new KafkaStreamsCreateViewPTransformOverrideFactory<>()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.emptyFlatten(), EmptyFlattenAsCreateFactory.instance()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory<>()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(
                        PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                    new SplittableParDoViaKeyedWorkItems.OverrideFactory<>()))
            .build());
    PipelineTranslator pipelineTranslator = new PipelineTranslator(pipeline, pipelineOptions);
    pipeline.traverseTopologically(pipelineTranslator);
    return TestKafkaStreamsPipelineResult.of(pipelineTranslator.finalize(this::finalize));
  }

  private Void finalize(TranslationContext context) {
    Topology topology = context.getStreamsBuilder().build();
    Properties config = new Properties();
    config.putAll(pipelineOptions.getProperties());
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, config);

    TestInputTopic<Void, WatermarkOrWindowedValue<byte[]>> impulseTopic =
        topologyTestDriver.createInputTopic(
            context.getImpulseTopic(),
            CoderSerde.of(VoidCoder.of()).serializer(),
            CoderSerde.of(
                    WatermarkOrWindowedValueCoder.of(
                        WindowedValue.getFullCoder(
                            ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE)))
                .serializer());
    try {
      impulseTopic.pipeInput(
          WatermarkOrWindowedValue.of(
              Watermark.of(BytesCoder.EMPTY, GlobalWindow.TIMESTAMP_MIN_VALUE)));
      impulseTopic.pipeInput(
          WatermarkOrWindowedValue.of(WindowedValue.valueInGlobalWindow(new byte[0])));
      impulseTopic.pipeInput(
          WatermarkOrWindowedValue.of(
              Watermark.of(BytesCoder.EMPTY, GlobalWindow.TIMESTAMP_MAX_VALUE)));

      while (notFinished(topologyTestDriver)) {
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));
      }
      topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));
    } catch (StreamsException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        cause = cause.getCause();
        if (cause instanceof UserCodeException) {
          cause = cause.getCause();
        }
      }
      throw new RuntimeException(cause == null ? "" : cause.getMessage(), e);
    }
    topologyTestDriver.close();
    return null;
  }

  private boolean notFinished(TopologyTestDriver topologyTestDriver) {
    for (Entry<String, StateStore> entry : topologyTestDriver.getAllStateStores().entrySet()) {
      if (entry.getKey().endsWith("_Timer")) {
        KeyValueStore<Object, Map<String, TimerInternals.TimerData>> store =
            (KeyValueStore<Object, Map<String, TimerInternals.TimerData>>) entry.getValue();
        KTimerInternals<Object> timerInternals = new KTimerInternals<>(store);
        timerInternals.advanceInputWatermarkTime();
        if (timerInternals.currentInputWatermarkTime().isBefore(GlobalWindow.TIMESTAMP_MAX_VALUE)) {
          return true;
        }
      }
    }
    return false;
  }
}
