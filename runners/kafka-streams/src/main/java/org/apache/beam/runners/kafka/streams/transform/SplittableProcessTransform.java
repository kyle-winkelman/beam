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
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces.GlobalNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowAndTriggerNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.TimerInternals.TimerData;
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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SplittableProcessTransform<
        InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
    extends StatefulParDoTransform<
        byte[], KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> {

  private final DoFn<InputT, OutputT> internalDoFn;
  private final String named;
  private final Consumer<KV<String, KV<byte[], Instant>>> watermarkConsumer;

  private ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT> processFn;

  public SplittableProcessTransform(
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
      Set<String> streamSource,
      DoFn<InputT, OutputT> internalDoFn,
      String named,
      Consumer<KV<String, KV<byte[], Instant>>> watermarkConsumer) {
    super(
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
        timerStoreName,
        streamSource);
    this.internalDoFn = internalDoFn;
    this.named = named;
    this.watermarkConsumer = watermarkConsumer;
  }

  @Override
  public void init(ProcessorContext context) {
    processFn =
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
            Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory()),
            1000,
            Duration.millis(1000),
            () -> bundleFinalizer));
    processFn.setWatermarkConsumer(watermark -> watermarkConsumer.accept(KV.of(named, watermark)));
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
  protected void setKey(
      WindowedValue<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> windowedValue) {
    key = windowedValue.getValue().key();
  }

  @Override
  protected void fireTimers(byte[] key, List<TimerData> timerDatas) {
    this.key = key;
    for (TimerData timerData : timerDatas) {
      BoundedWindow window;
      StateNamespace namespace = timerData.getNamespace();
      if (namespace instanceof GlobalNamespace) {
        window = GlobalWindow.INSTANCE;
      } else if (namespace instanceof WindowNamespace) {
        window = ((WindowNamespace<?>) namespace).getWindow();
      } else if (namespace instanceof WindowAndTriggerNamespace) {
        window = ((WindowAndTriggerNamespace<?>) namespace).getWindow();
      } else {
        throw new RuntimeException("Invalid namespace: " + namespace);
      }
      Iterables.addAll(
          pushbacks,
          doFnRunner.processElementInReadyWindows(
              WindowedValue.of(
                  KeyedWorkItems.timersWorkItem(key, Collections.singleton(timerData)),
                  timerData.getOutputTimestamp(),
                  window,
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
      context.forward(mainOutputTag, WindowedValue.of(output, timestamp, windows, pane));
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      context.forward(tag, WindowedValue.of(output, timestamp, windows, pane));
    }
  }
}
