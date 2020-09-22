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
package org.apache.beam.runners.kafka.streams.serde;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Assert;
import org.junit.Test;

/** JUnit Test for {@link CoderSerde}, {@link CoderSerializer}, and {@link CoderDeserializer}. */
public class CoderSerdeTest {

  private static class MockCoder extends Coder<Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public void encode(Integer value, OutputStream outStream) throws CoderException, IOException {
      throw new IOException();
    }

    @Override
    public Integer decode(InputStream inStream) throws CoderException, IOException {
      throw new IOException();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  @Test
  public void testIntegerCoder() {
    int data = 1;
    Serde<Integer> integerSerde = CoderSerde.of(VarIntCoder.of());
    integerSerde.configure(null, false);

    Serializer<Integer> integerSerializer = integerSerde.serializer();
    integerSerializer.configure(null, false);
    byte[] bytes = integerSerializer.serialize(null, data);
    integerSerializer.close();

    Deserializer<Integer> integerDeserializer = integerSerde.deserializer();
    integerDeserializer.configure(null, false);
    Assert.assertEquals(data, (int) integerDeserializer.deserialize(null, bytes));
    integerDeserializer.close();

    integerSerde.close();
  }

  @Test(expected = RuntimeException.class)
  public void testMockCoderEncode() {
    int data = 1;
    Serializer<Integer> integerSerializer = CoderSerde.of(new MockCoder()).serializer();
    integerSerializer.serialize(null, data);
  }

  @Test(expected = RuntimeException.class)
  public void testMockCoderDecode() {
    byte[] data = new byte[0];
    Deserializer<Integer> integerDeserializer = CoderSerde.of(new MockCoder()).deserializer();
    integerDeserializer.deserialize(null, data);
  }
}
