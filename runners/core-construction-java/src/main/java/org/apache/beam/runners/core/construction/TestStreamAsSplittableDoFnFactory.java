package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.Event;
import org.apache.beam.sdk.testing.TestStream.ProcessingTimeEvent;
import org.apache.beam.sdk.testing.TestStream.TestStreamCoder;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class TestStreamAsSplittableDoFnFactory<T> implements PTransformOverrideFactory<PBegin, PCollection<T>, TestStream<T>> {

  @Override
  public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
      AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> transform) {
    return PTransformReplacement.of(PBegin.in(transform.getPipeline()), new TestStreamAsSplittableDoFnPTransform<>(transform.getTransform()));
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }

  private static class TestStreamAsSplittableDoFnPTransform<T> extends PTransform<PBegin, PCollection<T>> {

    private final TestStream<T> testStream;

    public TestStreamAsSplittableDoFnPTransform(TestStream<T> testStream) {
      this.testStream = testStream;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      Coder<TestStream<T>> testStreamCoder = TestStreamCoder.of(testStream.getValueCoder());
      return input.getPipeline().apply(Create.of(testStream).withCoder(testStreamCoder))
          .apply(ParDo.of(new TestStreamAsSplittableDoFn<>(testStreamCoder)))
          .setCoder(testStream.getValueCoder());
    }
  }

  @UnboundedPerElement
  private static class TestStreamAsSplittableDoFn<T> extends DoFn<TestStream<T>, T> {

    private static final int DEFAULT_DESIRED_NUM_SPLITS = 20;

    private final Coder<TestStream<T>> testStreamCoder;

    private TestStreamAsSplittableDoFn(Coder<TestStream<T>> testStreamCoder) {
      this.testStreamCoder = testStreamCoder;
    }

    @GetInitialRestriction
    public TestStream<T> initialRestriction(@Element TestStream<T> element) {
      return element;
    }

    @GetRestrictionCoder
    public Coder<TestStream<T>> restrictionCoder() {
      return testStreamCoder;
    }
    
    @SplitRestriction
    public void splitRestriction(
        @Restriction TestStream<T> restriction,
        OutputReceiver<TestStream<T>> receiver) {
      List<List<Event<T>>> splits = new ArrayList<>(DEFAULT_DESIRED_NUM_SPLITS);
      for (int i = 0; i < DEFAULT_DESIRED_NUM_SPLITS; i++) {
        splits.add(new ArrayList<>());
      }
      for (Event<T> event : restriction.getEvents()) {
        switch (event.getType()) {
        case ELEMENT:
          ElementEvent<T> element = (ElementEvent<T>) event;
          List<List<TimestampedValue<T>>> partitions = partition(element.getElements(), DEFAULT_DESIRED_NUM_SPLITS);
          for (int i = 0; i < DEFAULT_DESIRED_NUM_SPLITS; i++) {
            splits.get(i).add(ElementEvent.add(partitions.get(i)));
          }
          break;
        case PROCESSING_TIME:
        case WATERMARK:
          for (int i = 0; i < DEFAULT_DESIRED_NUM_SPLITS; i++) {
            splits.get(i).add(event);
          }
          break;
        default:
          throw new IllegalArgumentException("Unknown event type: " + event.getType());
        }
      }
      for (int i = 0; i < DEFAULT_DESIRED_NUM_SPLITS; i++) {
        receiver.output(TestStream.fromRawEvents(restriction.getValueCoder(), splits.get(i)));
      }
    }

    private List<List<TimestampedValue<T>>> partition(Iterable<TimestampedValue<T>> iterable, int size) {
      List<List<TimestampedValue<T>>> partitions = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        partitions.add(new ArrayList<>());
      }
      Iterator<TimestampedValue<T>> iterator = iterable.iterator();
      for (int i = 0; i < Iterables.size(iterable); i++) {
        partitions.get(i % size).add(iterator.next());
      }
      return partitions;
    }

    @NewTracker
    public RestrictionTracker<TestStream<T>, Void> restrictionTracker(
        @Restriction TestStream<T> restriction) {
      return new TestStreamAsSplittableDoFnRestrictionTracker<>(restriction);
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState) {
      return new WatermarkEstimators.Manual(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        RestrictionTracker<TestStream<T>, Void> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator,
        OutputReceiver<T> receiver) {
      TestStream<T> testStream = tracker.currentRestriction();

      while (tracker.tryClaim(null)) {
        Event<T> event = testStream.getEvents().remove(0);
        switch (event.getType()) {
        case ELEMENT:
          ElementEvent<T> element = (ElementEvent<T>) event;
          element.getElements().forEach(e -> receiver.outputWithTimestamp(e.getValue(), e.getTimestamp()));
          break;
        case PROCESSING_TIME:
          ProcessingTimeEvent<T> processingTime = (ProcessingTimeEvent<T>) event;
          return ProcessContinuation.resume().withResumeDelay(processingTime.getProcessingTimeAdvance());
        case WATERMARK:
          WatermarkEvent<T> watermark = (WatermarkEvent<T>) event;
          watermarkEstimator.setWatermark(watermark.getWatermark());
          return ProcessContinuation.resume();
        default:
          throw new IllegalArgumentException("Unknown event type: " + event.getType());
        }
      }
      return ProcessContinuation.stop();
    }
  }

  private static class TestStreamAsSplittableDoFnRestrictionTracker<T> extends RestrictionTracker<TestStream<T>, Void> {

    private TestStream<T> testStream;

    private TestStreamAsSplittableDoFnRestrictionTracker(TestStream<T> testStream) {
      this.testStream = testStream;
    }

    @Override
    public boolean tryClaim(Void position) {
      return !testStream.getEvents().isEmpty();
    }

    @Override
    public TestStream<T> currentRestriction() {
      return testStream;
    }

    @Override
    public @Nullable SplitResult<TestStream<T>> trySplit(double fractionOfRemainder) {
      TestStream<T> residual = testStream;
      testStream = TestStream.fromRawEvents(testStream.getValueCoder(), Collections.emptyList());
      return SplitResult.of(testStream, residual);
    }

    @Override
    public void checkDone() throws IllegalStateException {
      checkState(
          testStream.getEvents().isEmpty());
    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }
}
