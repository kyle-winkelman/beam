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
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.kafka.common.utils.Bytes;

public class BytesCoder extends StructuredCoder<Bytes> {

  public static final Bytes EMPTY = Bytes.wrap(Bytes.EMPTY);

  public static BytesCoder of() {
    return INSTANCE;
  }

  private static final BytesCoder INSTANCE = new BytesCoder();
  private static final Coder<byte[]> INNER = NullableCoder.of(ByteArrayCoder.of());

  private BytesCoder() {}

  @Override
  public void encode(Bytes value, OutputStream outStream) throws CoderException, IOException {
    if (value == null) {
      INNER.encode(null, outStream);
    } else {
      INNER.encode(value.get(), outStream);
    }
  }

  @Override
  public Bytes decode(InputStream inStream) throws CoderException, IOException {
    byte[] bytes = INNER.decode(inStream);
    if (bytes == null) {
      return null;
    } else {
      return Bytes.wrap(bytes);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    INNER.verifyDeterministic();
  }
}
