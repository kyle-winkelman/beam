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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryBundleFinalizer;
import org.apache.beam.runners.core.InMemoryBundleFinalizer.Finalization;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.kafka.streams.sideinput.KSideInputReader;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.joda.time.Instant;

public class ParDoTransform<InputT, OutputT>
    implements Transformer<
        Void,
        WatermarkOrWindowedValue<InputT>,
        KeyValue<TupleTag<?>, WatermarkOrWindowedValue<?>>> {

  protected final String named;
  protected final PipelineOptions pipelineOptions;
  protected final DoFn<InputT, OutputT> doFn;
  protected final Map<PCollectionView<?>, String> sideInputReaderMap;
  protected final TupleTag<OutputT> mainOutputTag;
  protected final List<TupleTag<?>> additionalOutputTags;
  protected final Coder<InputT> inputCoder;
  protected final Map<TupleTag<?>, Coder<?>> outputCoders;
  protected final WindowingStrategy<?, BoundedWindow> windowingStrategy;
  protected final DoFnSchemaInformation doFnSchemaInformation;
  protected final Map<String, PCollectionView<?>> sideInputMapping;

  protected ProcessorContext context;
  protected KSideInputReader sideInputReader;
  protected DoFnInvoker<InputT, OutputT> doFnInvoker;
  protected PushbackSideInputDoFnRunner<InputT, OutputT> doFnRunner;
  protected InMemoryBundleFinalizer bundleFinalizer;
  protected List<WindowedValue<InputT>> pushbacks;

  public ParDoTransform(
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
      Map<String, PCollectionView<?>> sideInputMapping) {
    this.named = named;
    this.pipelineOptions = pipelineOptions;
    this.doFn = SerializableUtils.clone(doFn);
    this.sideInputReaderMap = sideInputReaderMap;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.windowingStrategy = windowingStrategy;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, punctuator());
    sideInputReader = KSideInputReader.of(this.context, sideInputReaderMap);
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnRunner =
        SimplePushbackSideInputDoFnRunner.create(
            doFnRunner(), sideInputReaderMap.keySet(), sideInputReader);
    bundleFinalizer = new InMemoryBundleFinalizer();
    pushbacks = new ArrayList<>();
    tryOrTeardown(doFnInvoker::invokeSetup);
    tryOrTeardown(doFnRunner::startBundle);
  }

  protected Punctuator punctuator() {
    return new ParDoPunctuator();
  }

  protected StepContext stepContext() {
    return new ParDoStepContext();
  }

  protected DoFnRunner<InputT, OutputT> doFnRunner() {
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
      Void object, WatermarkOrWindowedValue<InputT> watermarkOrWindowedValue) {
    if (watermarkOrWindowedValue.windowedValue() == null) {
      watermark(watermarkOrWindowedValue);
    } else {
      System.out.println(named + ": " + watermarkOrWindowedValue);
      tryOrTeardown(
          () ->
              Iterables.addAll(
                  pushbacks,
                  doFnRunner.processElementInReadyWindows(
                      watermarkOrWindowedValue.windowedValue())));
    }
    return null;
  }

  @Override
  public void close() {
    tryOrTeardown(doFnRunner::finishBundle);
    finalizeBundles();
    doFnInvoker.invokeTeardown();
  }

  protected void watermark(WatermarkOrWindowedValue watermarkOrWindowedValue) {
    context.forward(mainOutputTag, watermarkOrWindowedValue);
    for (TupleTag<?> additionalOutputTag : additionalOutputTags) {
      context.forward(additionalOutputTag, watermarkOrWindowedValue);
    }
  }

  private void finalizeBundles() {
    for (Finalization finalization : bundleFinalizer.getAndClearFinalizations()) {
      if (finalization.getExpiryTime().isBefore(Instant.now())) {
        try {
          finalization.getCallback().onBundleSuccess();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  protected void tryOrTeardown(TryOrTeardown tryOrTeardown) {
    try {
      tryOrTeardown.tryOrTeardown();
    } catch (Exception e) {
      try {
        doFnInvoker.invokeTeardown();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
  }

  @FunctionalInterface
  protected interface TryOrTeardown {
    void tryOrTeardown();
  }

  protected class ParDoOutputManager implements DoFnRunners.OutputManager {

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      context.forward(tag, WatermarkOrWindowedValue.of(output));
    }
  }

  protected class ParDoPunctuator implements Punctuator {

    @Override
    public void punctuate(long timestamp) {
      tryOrTeardown(doFnRunner::finishBundle);
      finalizeBundles();
      tryOrTeardown(doFnRunner::startBundle);
      List<WindowedValue<InputT>> currentPushbacks = pushbacks;
      pushbacks = new ArrayList<>();
      for (WindowedValue<InputT> pushback : currentPushbacks) {
        transform(null, WatermarkOrWindowedValue.of(pushback));
      }
    }
  }

  private class ParDoStepContext implements StepContext {

    @Override
    public StateInternals stateInternals() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return bundleFinalizer;
    }
  }
}
