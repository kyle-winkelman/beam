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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.kafka.streams.watermark.Watermark;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SplittableProcessTransform<
        InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
    extends StatefulParDoTransform<
        byte[], KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> {

  private final DoFn<InputT, OutputT> internalDoFn;

  public SplittableProcessTransform(
      String named,
      PipelineOptions pipelineOptions,
      DoFn<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> doFn,
      Map<PCollectionView<?>, String> sideInputReaderMap,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Coder<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, BoundedWindow> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping,
      String stateStoreName,
      String timerStoreName,
      DoFn<InputT, OutputT> internalDoFn) {
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
        sideInputMapping,
        stateStoreName,
        timerStoreName);
    this.internalDoFn = internalDoFn;
  }

  @Override
  public void init(ProcessorContext context) {
    ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT> processFn =
        (ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>) doFn;
    processFn.setStateInternalsFactory(key -> stateInternals.withKey(key));
    processFn.setTimerInternalsFactory(key -> timerInternals.withKey(key));
    processFn.setProcessElementInvoker(
        new OutputAndTimeBoundedSplittableProcessElementInvoker<
            InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>(
            internalDoFn,
            pipelineOptions,
            new SplittableProcessOutputWindowedValue(),
            sideInputReader,
            Executors.newSingleThreadScheduledExecutor(),
            1000,
            Duration.standardSeconds(1),
            () -> bundleFinalizer));
    processFn.setWatermarkConsumer(new SplittableProcessWatermarkConsumer());
    super.init(context);
  }

  @Override
  protected DoFnRunner<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> doFnRunner() {
    return DoFnRunners.simpleRunner(
        pipelineOptions,
        doFn,
        sideInputReader,
        new ParDoOutputManager(),
        mainOutputTag,
        additionalOutputTags,
        stepContext(),
        inputCoder,
        outputCoders,
        windowingStrategy,
        doFnSchemaInformation,
        sideInputMapping);
  }

  @Override
  public KeyValue<TupleTag<?>, WatermarkOrWindowedValue<?>> transform(
      Void object,
      WatermarkOrWindowedValue<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>>
          watermarkOrWindowedValue) {
    if (watermarkOrWindowedValue.watermark() == null) {
      watermark(
          WatermarkOrWindowedValue.of(
              Watermark.of(
                  Bytes.wrap(watermarkOrWindowedValue.windowedValue().getValue().key()),
                  GlobalWindow.TIMESTAMP_MIN_VALUE)));
    }
    return super.transform(object, watermarkOrWindowedValue);
  }

  @Override
  protected void setKey(
      WindowedValue<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> windowedValue) {
    key = windowedValue.getValue().key();
  }

  @Override
  protected void fireTimers(byte[] key, List<TimerData> timers) {
    this.key = key;
    for (TimerData timerData : timers) {
      Iterables.addAll(
          pushbacks,
          doFnRunner.processElementInReadyWindows(
              WindowedValue.of(
                  KeyedWorkItems.timersWorkItem(key, Collections.singleton(timerData)),
                  timerData.getOutputTimestamp(),
                  timerDataWindow(timerData),
                  PaneInfo.NO_FIRING)));
    }
  }

  private class SplittableProcessOutputWindowedValue implements OutputWindowedValue<OutputT> {

    @Override
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      context.forward(
          mainOutputTag,
          WatermarkOrWindowedValue.of(WindowedValue.of(output, timestamp, windows, pane)));
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      context.forward(
          tag, WatermarkOrWindowedValue.of(WindowedValue.of(output, timestamp, windows, pane)));
    }
  }

  private class SplittableProcessWatermarkConsumer implements Consumer<KV<byte[], Instant>> {

    @Override
    public void accept(KV<byte[], Instant> kv) {
      Instant watermark = kv.getValue();
      if (watermark == null) {
        watermark = timerInternals.currentInputWatermarkTime();
      }
      watermark(WatermarkOrWindowedValue.of(Watermark.of(Bytes.wrap(kv.getKey()), watermark)));
    }
  }
}
