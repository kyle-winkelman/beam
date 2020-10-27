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
package org.apache.beam.runners.kafka.streams.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.runners.kafka.streams.watermark.Watermark;
import org.apache.beam.runners.kafka.streams.watermark.WatermarkOrWindowedValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class WatermarkOrWindowedValueCoder<T> extends StructuredCoder<WatermarkOrWindowedValue<T>> {

  public static <T> WatermarkOrWindowedValueCoder<T> of(
      Coder<WindowedValue<T>> windowedValueCoder) {
    return new WatermarkOrWindowedValueCoder<T>(windowedValueCoder);
  }

  private final Coder<WindowedValue<T>> windowedValueCoder;
  private final UnionCoder unionCoder;

  private WatermarkOrWindowedValueCoder(Coder<WindowedValue<T>> windowedValueCoder) {
    this.windowedValueCoder = windowedValueCoder;
    this.unionCoder = UnionCoder.of(ImmutableList.of(WatermarkCoder.of(), windowedValueCoder));
  }

  @Override
  public void encode(WatermarkOrWindowedValue<T> value, OutputStream outStream)
      throws CoderException, IOException {
    if (value.windowedValue() == null) {
      unionCoder.encode(new RawUnionValue(0, value.watermark()), outStream);
    } else {
      unionCoder.encode(new RawUnionValue(1, value.windowedValue()), outStream);
    }
  }

  @Override
  public WatermarkOrWindowedValue<T> decode(InputStream inStream)
      throws CoderException, IOException {
    RawUnionValue union = unionCoder.decode(inStream);
    if (union.getUnionTag() == 0) {
      return WatermarkOrWindowedValue.of((Watermark) union.getValue());
    } else {
      return WatermarkOrWindowedValue.of((WindowedValue<T>) union.getValue());
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return windowedValueCoder.getCoderArguments();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    unionCoder.verifyDeterministic();
  }
}
