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
import java.util.Collections;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KOrderedListTest {

  private static final String KEY = "KEY";
  private static final String ID = "ID";
  private static final Instant INSTANT_ONE = Instant.ofEpochMilli(1);
  private static final Instant INSTANT_TWO = Instant.ofEpochMilli(2);
  private static final Instant INSTANT_THREE = Instant.ofEpochMilli(3);
  private static final TimestampedValue<Integer> VALUE_ONE = TimestampedValue.of(1, INSTANT_ONE);
  private static final TimestampedValue<Integer> VALUE_TWO = TimestampedValue.of(2, INSTANT_TWO);

  private MockKeyValueStore<KV<String, String>, byte[]> store;
  private KOrderedListState<String, Integer> orderedListState;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    orderedListState =
        new KOrderedListState<String, Integer>(
            KEY, StateNamespaces.global(), ID, store, VarIntCoder.of());
  }

  @Test
  public void testAddFirstAndAdditionalThenRead() {
    orderedListState.add(VALUE_TWO);
    orderedListState.add(VALUE_ONE);
    Assert.assertEquals(Arrays.asList(VALUE_ONE, VALUE_TWO), orderedListState.read());
    Assert.assertEquals(
        Arrays.asList(VALUE_ONE), orderedListState.readRange(INSTANT_ONE, INSTANT_TWO));
    Assert.assertEquals(
        Arrays.asList(VALUE_ONE, VALUE_TWO),
        orderedListState.readRange(INSTANT_ONE, INSTANT_THREE));
  }

  @Test
  public void testCheckIsEmptyBeforeAndAfterClear() {
    orderedListState.add(VALUE_ONE);
    Assert.assertFalse(orderedListState.isEmpty().readLater().read());
    orderedListState.clear();
    Assert.assertTrue(orderedListState.isEmpty().readLater().read());
  }

  @Test
  public void testCheckIsEmptyBeforeAndAfterClearRange() {
    orderedListState.clearRange(INSTANT_ONE, INSTANT_TWO);
    Assert.assertTrue(orderedListState.isEmpty().readLater().read());
    orderedListState.add(VALUE_ONE);
    orderedListState.add(VALUE_TWO);
    Assert.assertFalse(orderedListState.isEmpty().readLater().read());
    orderedListState.clearRange(INSTANT_TWO, INSTANT_THREE);
    Assert.assertFalse(orderedListState.isEmpty().readLater().read());
    orderedListState.clearRange(INSTANT_ONE, INSTANT_TWO);
    Assert.assertTrue(orderedListState.isEmpty().readLater().read());
  }

  @Test
  public void testReadEmpty() {
    Assert.assertEquals(Collections.emptyList(), orderedListState.readLater().read());
    Assert.assertEquals(
        Collections.emptyList(),
        orderedListState
            .readRangeLater(INSTANT_ONE, INSTANT_TWO)
            .readRange(INSTANT_ONE, INSTANT_TWO));
  }
}
