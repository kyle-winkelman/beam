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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link org.apache.beam.runners.kafka.streams.state.KSetState}. */
public class KSetStateTest {

  private static final String KEY = "KEY";
  private static final String ID = "ID";
  private static final Integer VALUE_ONE = 1;
  private static final Integer VALUE_TWO = 2;

  private MockKeyValueStore<KV<String, String>, byte[]> store;
  private KSetState<String, Integer> setState;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    setState =
        new KSetState<String, Integer>(KEY, StateNamespaces.global(), ID, store, VarIntCoder.of());
  }

  @Test
  public void testAddFirstAndAdditionalThenRead() {
    Set<Integer> expected = new HashSet<>();
    expected.add(VALUE_ONE);
    expected.add(VALUE_TWO);
    setState.add(VALUE_ONE);
    setState.add(VALUE_TWO);
    Assert.assertEquals(expected, setState.readLater().read());
  }

  @Test
  public void testAddIfAbsent() {
    Assert.assertTrue(setState.addIfAbsent(VALUE_ONE).readLater().read());
    Assert.assertFalse(setState.addIfAbsent(VALUE_ONE).readLater().read());
  }

  @Test
  public void testContains() {
    Assert.assertFalse(setState.contains(VALUE_ONE).readLater().read());
    setState.add(VALUE_ONE);
    Assert.assertTrue(setState.contains(VALUE_ONE).readLater().read());
    Assert.assertFalse(setState.contains(VALUE_TWO).readLater().read());
  }

  @Test
  public void testCheckIsEmptyBeforeAndAfterClear() {
    setState.add(VALUE_ONE);
    Assert.assertFalse(setState.isEmpty().readLater().read());
    setState.clear();
    Assert.assertTrue(setState.isEmpty().readLater().read());
  }

  @Test
  public void testRemoveEntryAndEntireSet() {
    setState.add(VALUE_ONE);
    setState.add(VALUE_TWO);
    setState.remove(VALUE_ONE);
    setState.remove(VALUE_TWO);
    Assert.assertEquals(Collections.emptySet(), setState.readLater().read());
  }

  @Test
  public void testRemoveEntryThatDoesntExistWhenEmpty() {
    setState.remove(VALUE_ONE);
    Assert.assertEquals(Collections.emptySet(), setState.readLater().read());
  }

  @Test
  public void testRemoveEntryThatDoesntExistWhenOtherEntriesExist() {
    setState.add(VALUE_ONE);
    setState.remove(VALUE_TWO);
    Assert.assertEquals(Collections.singleton(VALUE_ONE), setState.readLater().read());
  }
}
