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
package org.apache.beam.runners.kafka.streams.override;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class KafkaStreamsCreateViewPTransformOverrideFactory<ElemT, ViewT>
    implements PTransformOverrideFactory<
        PCollection<ElemT>, PCollection<ElemT>, View.CreatePCollectionView<ElemT, ViewT>> {

  public static final String KAFKA_STREAMS_CREATE_VIEW_URN =
      "beam:transform:kafka_streams:create_view:v1";

  public KafkaStreamsCreateViewPTransformOverrideFactory() {}

  @Override
  public PTransformReplacement<PCollection<ElemT>, PCollection<ElemT>> getReplacementTransform(
      AppliedPTransform<
              PCollection<ElemT>, PCollection<ElemT>, View.CreatePCollectionView<ElemT, ViewT>>
          transform) {
    PCollection<ElemT> collection =
        (PCollection<ElemT>) Iterables.getOnlyElement(transform.getInputs().values());
    return PTransformReplacement.of(
        collection,
        new PTransform<PCollection<ElemT>, PCollection<ElemT>>() {

          @Override
          public PCollection<ElemT> expand(PCollection<ElemT> input) {
            input
                .apply(
                    Combine.<ElemT, List<ElemT>>globally(new CreateViewCombineFn<>())
                        .withoutDefaults())
                .setCoder(ListCoder.of(input.getCoder()))
                .apply(new KafkaStreamsCreateView<>(transform.getTransform().getView()));
            return input;
          }
        });
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PValue> outputs, PCollection<ElemT> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }

  private static class CreateViewCombineFn<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }

  public static class KafkaStreamsCreateView<ElemT, ViewT>
      extends RawPTransform<PCollection<List<ElemT>>, PCollection<List<ElemT>>> {

    private PCollectionView<ViewT> view;

    private KafkaStreamsCreateView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    @Override
    public PCollection<List<ElemT>> expand(PCollection<List<ElemT>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), input.getCoder());
    }

    @Override
    public String getUrn() {
      return KAFKA_STREAMS_CREATE_VIEW_URN;
    }

    @Override
    public RunnerApi.FunctionSpec getSpec() {
      throw new UnsupportedOperationException(
          String.format("%s should never be serialized to proto", getClass().getSimpleName()));
    }

    public PCollectionView<ViewT> getView() {
      return view;
    }
  }
}
