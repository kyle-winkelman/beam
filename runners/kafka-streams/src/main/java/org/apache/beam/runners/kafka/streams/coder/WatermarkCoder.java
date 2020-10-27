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
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.kafka.streams.watermark.Watermark;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StructuredCoder;

public class WatermarkCoder extends StructuredCoder<Watermark> {

  public static WatermarkCoder of() {
    return INSTANCE;
  }

  private static final WatermarkCoder INSTANCE = new WatermarkCoder();
  private static final BytesCoder BYTES_CODER = BytesCoder.of();
  private static final InstantCoder INSTANT_CODER = InstantCoder.of();

  private WatermarkCoder() {}

  @Override
  public void encode(Watermark value, OutputStream outStream) throws CoderException, IOException {
    BYTES_CODER.encode(value.id(), outStream);
    INSTANT_CODER.encode(value.watermark(), outStream);
  }

  @Override
  public Watermark decode(InputStream inStream) throws CoderException, IOException {
    return Watermark.of(BYTES_CODER.decode(inStream), INSTANT_CODER.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    BYTES_CODER.verifyDeterministic();
    INSTANT_CODER.verifyDeterministic();
  }
}
