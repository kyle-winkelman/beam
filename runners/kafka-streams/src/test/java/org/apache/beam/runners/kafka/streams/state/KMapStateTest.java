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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link org.apache.beam.runners.kafka.streams.state.KMapState}. */
public class KMapStateTest {

  private static final String KEY = "KEY";
  private static final String ID = "ID";
  private static final String KEY_ONE = "KEY_ONE";
  private static final Integer VALUE_ONE = 1;
  private static final String KEY_TWO = "KEY_TWO";
  private static final Integer VALUE_TWO = 2;

  private KeyValueStore<KV<String, String>, byte[]> store;
  private KMapState<String, String, Integer> mapState;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    mapState =
        new KMapState<String, String, Integer>(
            KEY, StateNamespaces.global(), ID, store, StringUtf8Coder.of(), VarIntCoder.of());
  }

  @Test
  public void testClear() {
    mapState.put(KEY_ONE, VALUE_ONE);
    mapState.put(KEY_TWO, VALUE_TWO);
    mapState.clear();
    Assert.assertNull(mapState.get(KEY_ONE).readLater().read());
    Assert.assertNull(mapState.get(KEY_TWO).readLater().read());
  }

  @Test
  public void testPutFirstAndAdditionalThenRead() {
    Map<String, Integer> expected = new HashMap<>();
    expected.put(KEY_ONE, VALUE_ONE);
    expected.put(KEY_TWO, VALUE_TWO);
    mapState.put(KEY_ONE, VALUE_ONE);
    mapState.put(KEY_TWO, VALUE_TWO);
    Assert.assertEquals(VALUE_ONE, mapState.get(KEY_ONE).readLater().read());
    Assert.assertEquals(VALUE_TWO, mapState.get(KEY_TWO).readLater().read());
    Assert.assertEquals(expected.entrySet(), mapState.entries().readLater().read());
    Assert.assertEquals(expected.keySet(), mapState.keys().readLater().read());
    Assert.assertEquals(
        new HashSet<>(expected.values()),
        new HashSet<>(
            StreamSupport.stream(mapState.values().readLater().read().spliterator(), false)
                .collect(Collectors.toList())));
  }

  @Test
  public void testPutIfAbsent() {
    Assert.assertNull(mapState.putIfAbsent(KEY_ONE, VALUE_ONE).readLater().read());
    Assert.assertEquals(VALUE_ONE, mapState.putIfAbsent(KEY_ONE, VALUE_TWO).readLater().read());
  }

  @Test
  public void testReadEmpty() {
    Map<String, String> expected = new HashMap<>();
    Assert.assertNull(mapState.get(KEY_ONE).readLater().read());
    Assert.assertNull(mapState.get(KEY_TWO).readLater().read());
    Assert.assertEquals(expected.entrySet(), mapState.entries().readLater().read());
    Assert.assertEquals(expected.keySet(), mapState.keys().readLater().read());
    Assert.assertEquals(
        new HashSet<>(expected.values()),
        new HashSet<>(
            StreamSupport.stream(mapState.values().readLater().read().spliterator(), false)
                .collect(Collectors.toList())));
  }

  @Test
  public void testRemoveEntryAndEntireMap() {
    mapState.put(KEY_ONE, VALUE_ONE);
    mapState.put(KEY_TWO, VALUE_TWO);
    // Remove Entry.
    mapState.remove(KEY_ONE);
    // Remove Entire Map.
    mapState.remove(KEY_TWO);
    Assert.assertNull(mapState.get(KEY_ONE).readLater().read());
    Assert.assertNull(mapState.get(KEY_TWO).readLater().read());
  }

  @Test
  public void testRemoveEntryThatDoesNotExistWhenEmpty() {
    mapState.remove(KEY_ONE);
    Assert.assertNull(mapState.get(KEY_ONE).readLater().read());
  }

  @Test
  public void testRemoveEntryThatDoesntExistWhenOtherEntriesExist() {
    mapState.put(KEY_ONE, VALUE_ONE);
    mapState.remove(KEY_TWO);
    Assert.assertNull(mapState.get(KEY_TWO).readLater().read());
  }
}
