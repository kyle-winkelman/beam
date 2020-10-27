package org.apache.beam.runners.core.construction;

import java.util.Map;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
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
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
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
          .apply(ParDo.of(new TestStreamAsSplittableDoFn<>()))
          .setCoder(testStream.getValueCoder());
    }
  }

  @UnboundedPerElement
  private static class TestStreamAsSplittableDoFn<T> extends DoFn<TestStream<T>, T> {

    @GetInitialRestriction
    public OffsetRange initialRestriction(@Element TestStream<T> element) {
      return new OffsetRange(0, element.getEvents().size());
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
        @Element TestStream<T> testStream,
        RestrictionTracker<OffsetRange, Long> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator,
        OutputReceiver<T> receiver) {

      for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); i++) {
        Event<T> event = testStream.getEvents().get((int) i);
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
}
