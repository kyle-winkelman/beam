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

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

@AutoService(TransformPayloadTranslatorRegistrar.class)
public class KafkaStreamsTransformPayloadTranslatorRegistrar
    implements TransformPayloadTranslatorRegistrar {
  @Override
  public Map<
          ? extends Class<? extends PTransform>,
          ? extends PTransformTranslation.TransformPayloadTranslator>
      getTransformPayloadTranslators() {
    return ImmutableMap
        .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
        .put(
            SplittableParDoViaKeyedWorkItems.ProcessElements.class,
            PTransformTranslation.TransformPayloadTranslator.NotSerializable.forUrn(
                SplittableParDo.SPLITTABLE_PROCESS_URN))
        .build();
  }
}
