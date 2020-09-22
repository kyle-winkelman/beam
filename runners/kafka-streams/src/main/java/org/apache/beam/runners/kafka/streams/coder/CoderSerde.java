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

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CoderSerde<T> implements Serde<T> {

  public static <T> CoderSerde<T> of(Coder<T> coder) {
    return new CoderSerde<>(coder);
  }

  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  private CoderSerde(Coder<T> coder) {
    this.serializer = CoderSerializer.of(coder);
    this.deserializer = CoderDeserializer.of(coder);
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }

  /** Kafka {@link Serializer} that uses a Beam Coder to encode. */
  public static class CoderSerializer<T> implements Serializer<T> {

    public static <T> CoderSerializer<T> of(Coder<T> coder) {
      return new CoderSerializer<>(coder);
    }

    private final Coder<T> coder;

    private CoderSerializer(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, T data) {
      try {
        return CoderUtils.encodeToByteArray(coder, data);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Kafka {@link Deserializer} that uses a Beam Coder to decode. */
  public static class CoderDeserializer<T> implements Deserializer<T> {

    public static <T> CoderDeserializer<T> of(Coder<T> coder) {
      return new CoderDeserializer<>(coder);
    }

    private final Coder<T> coder;

    private CoderDeserializer(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public T deserialize(String topic, byte[] data) {
      try {
        return CoderUtils.decodeFromByteArray(coder, data);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
