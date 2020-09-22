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
package org.apache.beam.runners.kafka.streams.translation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public abstract class TransformTranslator<
    InputT extends PInput,
    OutputT extends POutput,
    TransformT extends PTransform<InputT, OutputT>> {

  private static final int MAX_NAME_LENGTH = 210;

  abstract void translate(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform, TranslationContext context);

  protected InputT mainInput(AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return (InputT) Iterables.getOnlyElement(mainInputs(appliedPTransform));
  }

  protected <T> List<PCollection<T>> mainInputs(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return TransformInputs.nonAdditionalInputs(appliedPTransform).stream()
        .map(pValue -> (PCollection<T>) pValue)
        .collect(Collectors.toList());
  }

  protected OutputT output(AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return (OutputT) Iterables.getOnlyElement(outputs(appliedPTransform).values());
  }

  protected <T> TupleTag<T> mainOutputTag(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    try {
      return (TupleTag<T>) ParDoTranslation.getMainOutputTag(appliedPTransform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Map<TupleTag<?>, PCollection<?>> outputs(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return appliedPTransform.getOutputs().entrySet().stream()
        .map(entry -> KV.of(entry.getKey(), (PCollection<?>) entry.getValue()))
        .collect(Collectors.toMap(kv -> kv.getKey(), kv -> kv.getValue()));
  }

  protected List<TupleTag<?>> additionalOutputTags(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return ImmutableList.copyOf(outputs(appliedPTransform).keySet());
  }

  protected Map<TupleTag<?>, Coder<?>> outputCoders(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return outputs(appliedPTransform).entrySet().stream()
        .map(entry -> KV.of(entry.getKey(), entry.getValue().getCoder()))
        .collect(Collectors.toMap(kv -> kv.getKey(), kv -> kv.getValue()));
  }

  protected <T> Coder<WindowedValue<T>> coder(PCollection<T> pCollection) {
    if (pCollection.getWindowingStrategy() == null) {
      return WindowedValue.getFullCoder(pCollection.getCoder(), GlobalWindow.Coder.INSTANCE);
    } else {
      return WindowedValue.getFullCoder(
          pCollection.getCoder(), pCollection.getWindowingStrategy().getWindowFn().windowCoder());
    }
  }

  protected <T> Coder<WindowedValue<T>> coder(PCollection<?> pCollection, Coder<T> coder) {
    if (pCollection.getWindowingStrategy() == null) {
      return WindowedValue.getFullCoder(coder, GlobalWindow.Coder.INSTANCE);
    } else {
      return WindowedValue.getFullCoder(
          coder, pCollection.getWindowingStrategy().getWindowFn().windowCoder());
    }
  }

  protected <K, V> KvCoder<K, V> kvCoder(PCollection<?> pCollection) {
    return (KvCoder<K, V>) pCollection.getCoder();
  }

  protected <DoFnInputT, DoFnOutputT> DoFn<DoFnInputT, DoFnOutputT> doFn(
      AppliedPTransform<InputT, OutputT, TransformT> appliedTransform) {
    try {
      return (DoFn<DoFnInputT, DoFnOutputT>) ParDoTranslation.getDoFn(appliedTransform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected <T> WindowingStrategy<T, BoundedWindow> windowingStrategy(PCollection<T> pCollection) {
    return (WindowingStrategy<T, BoundedWindow>) pCollection.getWindowingStrategy();
  }

  protected <T> Coder<BoundedWindow> windowCoder(PCollection<T> pCollection) {
    return windowingStrategy(pCollection).getWindowFn().windowCoder();
  }

  protected DoFnSchemaInformation doFnSchemaInformation(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return ParDoTranslation.getSchemaInformation(appliedPTransform);
  }

  protected Map<String, PCollectionView<?>> sideInputMapping(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    return ParDoTranslation.getSideInputMapping(appliedPTransform);
  }

  protected Map<String, PCollectionView<?>> sideInputMapping(
      List<PCollectionView<?>> pCollectionViews) {
    return pCollectionViews.stream()
        .map(pCollectionView -> KV.of(pCollectionView.getTagInternal().getId(), pCollectionView))
        .collect(Collectors.toMap(kv -> kv.getKey(), kv -> kv.getValue()));
  }

  protected boolean usesStateOrTimers(
      AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    try {
      return ParDoTranslation.usesStateOrTimers(appliedPTransform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected String named(AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
    String named =
        appliedPTransform
            .getFullName()
            .replaceAll("\\(", ".")
            .replaceAll("/", "_")
            .replaceAll("[^a-zA-Z0-9.-_]", "");
    if (named.length() > MAX_NAME_LENGTH) {
      named = named.substring(named.length() - MAX_NAME_LENGTH);
    }
    return named;
  }
}
