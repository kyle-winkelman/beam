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

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces.GlobalNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowAndTriggerNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StatefulDoFnRunner.StateInternalsStateCleaner;
import org.apache.beam.runners.core.StatefulDoFnRunner.TimeInternalsCleanupTimer;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.kafka.streams.state.KStateInternals;
import org.apache.beam.runners.kafka.streams.state.KTimerInternals;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.joda.time.Instant;

public class StatefulParDoTransform<K, InputT, OutputT> extends ParDoTransform<InputT, OutputT> {

  private final String stateStoreName;
  private final String timerStoreName;

  protected KStateInternals<K> stateInternals;
  protected KTimerInternals<K> timerInternals;
  protected K key;

  public StatefulParDoTransform(
      String named,
      PipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      Map<PCollectionView<?>, String> sideInputReaderMap,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, BoundedWindow> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping,
      String stateStoreName,
      String timerStoreName) {
    super(
        named,
        pipelineOptions,
        doFn,
        sideInputReaderMap,
        mainOutputTag,
        additionalOutputTags,
        inputCoder,
        outputCoders,
        windowingStrategy,
        doFnSchemaInformation,
        sideInputMapping);
    this.stateStoreName = stateStoreName;
    this.timerStoreName = timerStoreName;
  }

  @Override
  public void init(ProcessorContext context) {
    stateInternals = KStateInternals.of(stateStoreName, context);
    timerInternals = KTimerInternals.of(timerStoreName, context);
    super.init(context);
  }

  @Override
  protected Punctuator punctuator() {
    return new StatefulParDoPunctuator();
  }

  @Override
  protected StepContext stepContext() {
    return new StatefulParDoStepContext();
  }

  @Override
  protected DoFnRunner<InputT, OutputT> doFnRunner() {
    return DoFnRunners.defaultStatefulDoFnRunner(
        doFn,
        inputCoder,
        super.doFnRunner(),
        stepContext(),
        windowingStrategy,
        new TimeInternalsCleanupTimer<>(timerInternals, windowingStrategy),
        new StateInternalsStateCleaner<>(
            doFn, stateInternals, windowingStrategy.getWindowFn().windowCoder()),
        false);
  }

  @Override
  public KeyValue<TupleTag<?>, WatermarkOrWindowedValue<?>> transform(
      Void object, WatermarkOrWindowedValue<InputT> watermarkOrWindowedValue) {
    if (watermarkOrWindowedValue.windowedValue() == null) {
      timerInternals.watermark(watermarkOrWindowedValue.watermark());
    } else {
      setKey(watermarkOrWindowedValue.windowedValue());
    }
    return super.transform(object, watermarkOrWindowedValue);
  }

  protected void setKey(WindowedValue<InputT> windowedValue) {
    key = ((KV<K, ?>) windowedValue.getValue()).getKey();
  }

  protected void fireTimers(K key, List<TimerData> timers) {
    this.key = key;
    for (TimerData timer : timers) {
      doFnRunner.onTimer(
          timer.getTimerId(),
          timer.getTimerFamilyId(),
          key,
          timerDataWindow(timer),
          timer.getTimestamp(),
          timer.getOutputTimestamp(),
          timer.getDomain());
    }
  }

  protected BoundedWindow timerDataWindow(TimerData timerData) {
    StateNamespace namespace = timerData.getNamespace();
    if (namespace instanceof GlobalNamespace) {
      return GlobalWindow.INSTANCE;
    } else if (namespace instanceof WindowNamespace) {
      return ((WindowNamespace<?>) namespace).getWindow();
    } else if (namespace instanceof WindowAndTriggerNamespace) {
      return ((WindowAndTriggerNamespace<?>) namespace).getWindow();
    } else {
      throw new RuntimeException("Invalid namespace: " + namespace);
    }
  }

  private class StatefulParDoPunctuator extends ParDoPunctuator {

    @Override
    public void punctuate(long timestamp) {
      Instant previousInputWatermarkTime = timerInternals.currentInputWatermarkTime();
      Instant previousProcessingTime = timerInternals.currentProcessingTime();
      timerInternals.advanceInputWatermarkTime();
      timerInternals.advanceOutputWatermarkTime(previousInputWatermarkTime);
      timerInternals.advanceProcessingTime(new Instant(timestamp));
      timerInternals.advanceSynchronizedProcessingTime(previousProcessingTime);
      super.punctuate(timestamp);
      timerInternals.fireTimers(StatefulParDoTransform.this::fireTimers);
    }
  }

  private class StatefulParDoStepContext implements StepContext {

    @Override
    public StateInternals stateInternals() {
      return stateInternals.withKey(key);
    }

    @Override
    public TimerInternals timerInternals() {
      return timerInternals.withKey(key);
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return bundleFinalizer;
    }
  }
}
