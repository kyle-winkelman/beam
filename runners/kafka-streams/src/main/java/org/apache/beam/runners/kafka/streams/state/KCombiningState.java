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
package org.apache.beam.runners.kafka.streams.state;

import java.util.Arrays;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.kafka.streams.coder.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;

public class KCombiningState<K, InputT, AccumT, OutputT> extends KAbstractState<K, AccumT>
    implements CombiningState<InputT, AccumT, OutputT> {

  private final CombineFn<InputT, AccumT, OutputT> combineFn;

  protected KCombiningState(
      K key,
      StateNamespace namespace,
      String id,
      KeyValueStore<KV<K, String>, byte[]> store,
      Coder<AccumT> accumCoder,
      CombineFn<InputT, AccumT, OutputT> combineFn) {
    super(key, namespace, id, store, CoderSerde.of(accumCoder));
    this.combineFn = combineFn;
  }

  @Override
  public void add(InputT value) {
    AccumT current = get();
    if (current == null) {
      current = combineFn.createAccumulator();
    }
    current = combineFn.addInput(current, value);
    set(current);
  }

  @Override
  public void addAccum(AccumT accum) {
    AccumT current = get();
    if (current == null) {
      set(accum);
    } else {
      current = combineFn.mergeAccumulators(Arrays.asList(current, accum));
      set(current);
    }
  }

  @Override
  public void clear() {
    super.clear();
  }

  @Override
  public AccumT getAccum() {
    AccumT accum = get();
    if (accum == null) {
      return combineFn.createAccumulator();
    } else {
      return accum;
    }
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return new ReadableState<Boolean>() {
      @Override
      public Boolean read() {
        return get() == null;
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }

  @Override
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    return combineFn.mergeAccumulators(accumulators);
  }

  @Override
  public OutputT read() {
    AccumT accum = get();
    if (accum != null) {
      return combineFn.extractOutput(accum);
    } else {
      return combineFn.extractOutput(combineFn.createAccumulator());
    }
  }

  @Override
  public CombiningState<InputT, AccumT, OutputT> readLater() {
    return this;
  }
}
