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
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link org.apache.beam.runners.kafka.streams.state.KCombiningState}. */
public class KCombiningStateTest {

  private static class MockCombineFn extends CombineFn<Integer, Integer, Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public Integer createAccumulator() {
      return 0;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input) {
      return accumulator + input;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
      int merge = 0;
      for (Integer accumulator : accumulators) {
        merge += accumulator;
      }
      return merge;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
      return accumulator;
    }
  }

  private static final String KEY = "KEY";
  private static final String ID = "ID";
  private static final Integer VALUE_ONE = 1;
  private static final Integer VALUE_TWO = 2;

  private KeyValueStore<KV<String, String>, byte[]> store;
  private KCombiningState<String, Integer, Integer, Integer> combiningState;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    combiningState =
        new KCombiningState<String, Integer, Integer, Integer>(
            KEY, StateNamespaces.global(), ID, store, VarIntCoder.of(), new MockCombineFn());
  }

  @Test
  public void testAddAndAddAccumThenRead() {
    combiningState.add(VALUE_ONE);
    combiningState.addAccum(VALUE_TWO);
    Assert.assertEquals(VALUE_ONE + VALUE_TWO, (int) combiningState.readLater().getAccum());
    Assert.assertEquals(VALUE_ONE + VALUE_TWO, (int) combiningState.readLater().read());
  }

  @Test
  public void testAddAccumAndAddThenRead() {
    combiningState.addAccum(VALUE_ONE);
    combiningState.add(VALUE_TWO);
    Assert.assertEquals(VALUE_ONE + VALUE_TWO, (int) combiningState.readLater().getAccum());
    Assert.assertEquals(VALUE_ONE + VALUE_TWO, (int) combiningState.readLater().read());
  }

  @Test
  public void testCheckIsEmptyBeforeAndAfterClear() {
    combiningState.add(VALUE_ONE);
    Assert.assertFalse(combiningState.isEmpty().readLater().read());
    combiningState.clear();
    Assert.assertTrue(combiningState.isEmpty().readLater().read());
  }

  @Test
  public void testReadEmpty() {
    Assert.assertEquals(0, (int) combiningState.readLater().getAccum());
    Assert.assertEquals(0, (int) combiningState.readLater().read());
  }

  @Test
  public void testMergeAccumulators() {
    Assert.assertEquals(
        VALUE_ONE + VALUE_TWO,
        (int) combiningState.mergeAccumulators(Arrays.asList(VALUE_ONE, VALUE_TWO)));
  }
}
