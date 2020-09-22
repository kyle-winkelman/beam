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

import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.LoggerFactory;

public class PipelineTranslator extends Pipeline.PipelineVisitor.Defaults {

  private final Pipeline pipeline;
  private final TranslationContext context;

  public PipelineTranslator(Pipeline pipeline, KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipeline = pipeline;
    this.context = new TranslationContext(pipelineOptions);
  }

  public <T> T finalize(TranslationFinalizer<T> finalizer) {
    return finalizer.finalize(context);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      if (TransformTranslators.has(PTransformTranslation.urnForTransformOrNull(transform))) {
        doVisitTransform(node);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    doVisitTransform(node);
  }

  private <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends PTransform<InputT, OutputT>>
      void doVisitTransform(Node node) {
    TransformT transform = (TransformT) node.getTransform();
    TransformTranslator<InputT, OutputT, TransformT> transformTranslator =
        (TransformTranslator<InputT, OutputT, TransformT>)
            TransformTranslators.get(PTransformTranslation.urnForTransformOrNull(transform));
    if (transformTranslator == null) {
      LoggerFactory.getLogger(getClass()).error("No TransformTranslator for {}", transform);
    }
    transformTranslator.translate(
        (AppliedPTransform<InputT, OutputT, TransformT>) node.toAppliedPTransform(pipeline),
        context);
  }
}
