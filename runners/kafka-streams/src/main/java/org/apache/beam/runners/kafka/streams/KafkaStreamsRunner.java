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
package org.apache.beam.runners.kafka.streams;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.EmptyFlattenAsCreateFactory;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.runners.kafka.streams.override.KafkaStreamsCreateViewPTransformOverrideFactory;
import org.apache.beam.runners.kafka.streams.translation.PipelineTranslator;
import org.apache.beam.runners.kafka.streams.translation.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.BeamTopicConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.joda.time.Instant;

public class KafkaStreamsRunner extends PipelineRunner<KafkaStreamsPipelineResult> {

  public static KafkaStreamsRunner fromOptions(PipelineOptions pipelineOptions) {
    PipelineOptionsValidator.validate(KafkaStreamsPipelineOptions.class, pipelineOptions);
    pipelineOptions.setStableUniqueNames(PipelineOptions.CheckEnabled.ERROR);
    return new KafkaStreamsRunner(pipelineOptions);
  }

  private final KafkaStreamsPipelineOptions pipelineOptions;

  private KafkaStreamsRunner(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions.as(KafkaStreamsPipelineOptions.class);
  }

  @Override
  public KafkaStreamsPipelineResult run(Pipeline pipeline) {
    pipeline.replaceAll(
        ImmutableList.<PTransformOverride>builder()
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
    return KafkaStreamsPipelineResult.of(pipelineTranslator.finalize(this::finalize));
  }

  private KafkaStreams finalize(TranslationContext context) {
    Topology topology = context.getStreamsBuilder().build();
    Properties properties = new Properties();
    properties.putAll(pipelineOptions.getProperties());

    Map<String, InternalTopicConfig> topics =
        ImmutableMap.<String, InternalTopicConfig>builder()
            .put(
                context.getImpulseTopic(),
                new BeamTopicConfig(context.getImpulseTopic(), Collections.emptyMap()))
            .put(
                context.getWatermarkTopic(),
                new BeamTopicConfig(context.getWatermarkTopic(), Collections.emptyMap()))
            .build();
    InternalTopicManager internalTopicManager =
        new InternalTopicManager(
            Admin.create(properties), new StreamsConfig(pipelineOptions.getProperties()));
    Set<String> newlyCreatedTopics = internalTopicManager.makeReady(topics);

    Producer<String, KV<byte[], Instant>> watermarkProducer =
        new KafkaProducer<String, KV<byte[], Instant>>(
            properties,
            CoderSerde.of(StringUtf8Coder.of()).serializer(),
            CoderSerde.of(KvCoder.of(ByteArrayCoder.of(), InstantCoder.of())).serializer());
    context
        .getWatermarkConsumer()
        .setConsumer(
            kv -> {
              try {
                watermarkProducer
                    .send(
                        new ProducerRecord<>(
                            context.getWatermarkTopic(), kv.getKey(), kv.getValue()))
                    .get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    if (newlyCreatedTopics.contains(context.getImpulseTopic())) {
      try (Producer<Void, WindowedValue<byte[]>> impulseProducer =
          new KafkaProducer<>(
              properties,
              CoderSerde.of(VoidCoder.of()).serializer(),
              CoderSerde.of(
                      WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE))
                  .serializer())) {
        try {
          impulseProducer
              .send(
                  new ProducerRecord<>(
                      context.getImpulseTopic(),
                      null,
                      BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis(),
                      null,
                      WindowedValue.valueInGlobalWindow(new byte[0])))
              .get();
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return new KafkaStreams(topology, properties);
  }
}
