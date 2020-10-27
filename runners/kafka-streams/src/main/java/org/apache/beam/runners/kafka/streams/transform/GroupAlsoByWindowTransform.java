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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFn;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.UnsupportedSideInputReader;
import org.apache.beam.runners.core.construction.TriggerTranslation;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.kafka.streams.state.KStateInternals;
import org.apache.beam.runners.kafka.streams.state.KTimerInternals;
import org.apache.beam.runners.kafka.streams.watermark.Watermark;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.joda.time.Instant;

public class GroupAlsoByWindowTransform<K, V>
    implements Transformer<
        K,
        WatermarkOrWindowedValue<V>,
        KeyValue<Void, WatermarkOrWindowedValue<KV<K, Iterable<V>>>>> {

  private final String named;
  private final PipelineOptions pipelineOptions;
  private final WindowingStrategy<KV<K, V>, BoundedWindow> windowingStrategy;
  private final ExecutableTriggerStateMachine triggerStateMachine;
  private final ReduceFn<K, V, Iterable<V>, BoundedWindow> reduceFn;
  private final String stateStoreName;
  private final String timerStoreName;
  private final Bytes bytes;

  private ProcessorContext context;
  private KStateInternals<K> stateInternals;
  private KTimerInternals<K> timerInternals;
  private boolean minWatermarkSent;

  public GroupAlsoByWindowTransform(
      String named,
      PipelineOptions pipelineOptions,
      WindowingStrategy<KV<K, V>, BoundedWindow> windowingStrategy,
      Coder<V> valueCoder,
      String stateStoreName,
      String timerStoreName) {
    this.named = named;
    this.pipelineOptions = pipelineOptions;
    this.windowingStrategy = windowingStrategy;
    this.triggerStateMachine =
        ExecutableTriggerStateMachine.create(
            TriggerStateMachines.stateMachineForTrigger(
                TriggerTranslation.toProto(windowingStrategy.getTrigger())));
    this.reduceFn = SystemReduceFn.buffering(valueCoder);
    this.stateStoreName = stateStoreName;
    this.timerStoreName = timerStoreName;
    byte[] bytes = new byte[128];
    ThreadLocalRandom.current().nextBytes(bytes);
    this.bytes = Bytes.wrap(bytes);
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.context.schedule(
        Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new GroupAlsoByWindowPunctuator());
    stateInternals = KStateInternals.of(stateStoreName, context);
    timerInternals = KTimerInternals.of(timerStoreName, context);
    minWatermarkSent = false;
  }

  @Override
  public KeyValue<Void, WatermarkOrWindowedValue<KV<K, Iterable<V>>>> transform(
      K key, WatermarkOrWindowedValue<V> watermarkOrWindowedValue) {
    if (!minWatermarkSent) {
      minWatermarkSent = true;
      watermarks(
          WatermarkOrWindowedValue.of(Watermark.of(bytes, GlobalWindow.TIMESTAMP_MIN_VALUE)));
    }
    if (watermarkOrWindowedValue.windowedValue() == null) {
      timerInternals.watermark(watermarkOrWindowedValue.watermark());
    } else {
      System.out.println(named + ": " + watermarkOrWindowedValue);
      ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner =
          new ReduceFnRunner<>(
              key,
              windowingStrategy,
              triggerStateMachine,
              stateInternals.withKey(key),
              timerInternals.withKey(key),
              new GroupAlsoByWindowOutputWindowedValue(),
              new UnsupportedSideInputReader(getClass().getName()),
              reduceFn,
              pipelineOptions);
      try {
        reduceFnRunner.processElements(
            Collections.singleton(watermarkOrWindowedValue.windowedValue()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public void close() {}

  private void watermarks(WatermarkOrWindowedValue<V> watermarkOrWindowedValue) {
    context.forward(null, watermarkOrWindowedValue);
  }

  private void fireTimers(K key, List<TimerData> timers) {
    ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner =
        new ReduceFnRunner<>(
            key,
            windowingStrategy,
            triggerStateMachine,
            stateInternals.withKey(key),
            timerInternals.withKey(key),
            new GroupAlsoByWindowOutputWindowedValue(),
            new UnsupportedSideInputReader(getClass().getName()),
            reduceFn,
            pipelineOptions);
    try {
      reduceFnRunner.onTimers(timers);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class GroupAlsoByWindowOutputWindowedValue
      implements OutputWindowedValue<KV<K, Iterable<V>>> {

    @Override
    public void outputWindowedValue(
        KV<K, Iterable<V>> output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      context.forward(
          null, WatermarkOrWindowedValue.of(WindowedValue.of(output, timestamp, windows, pane)));
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      throw new UnsupportedOperationException();
    }
  }

  private class GroupAlsoByWindowPunctuator implements Punctuator {

    @Override
    public void punctuate(long timestamp) {
      Instant previousInputWatermarkTime = timerInternals.currentInputWatermarkTime();
      Instant previousProcessingTime = timerInternals.currentProcessingTime();
      timerInternals.advanceInputWatermarkTime();
      timerInternals.advanceOutputWatermarkTime(previousInputWatermarkTime);
      timerInternals.advanceProcessingTime(new Instant(timestamp));
      timerInternals.advanceSynchronizedProcessingTime(previousProcessingTime);
      timerInternals.fireTimers(GroupAlsoByWindowTransform.this::fireTimers);
      watermarks(
          WatermarkOrWindowedValue.of(
              Watermark.of(bytes, timerInternals.currentInputWatermarkTime())));
    }
  }
}
