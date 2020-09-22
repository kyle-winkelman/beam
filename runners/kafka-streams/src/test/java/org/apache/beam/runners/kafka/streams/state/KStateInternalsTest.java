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
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link org.apache.beam.runners.kafka.streams.state.KStateInternals}. */
public class KStateInternalsTest {

  private static final String NAME = "NAME";
  private static final String KEY = "KEY";
  private static final String VALUE = "VALUE";
  private static final String BAG = "BAG";
  private static final String SET = "SET";
  private static final String MAP = "MAP";
  private static final String ORDERED_LIST = "ORDERED_LIST";
  private static final String COMBINING = "COMBINING";
  private static final String COMBINING_WITH_CONTEXT = "COMBINING_WITH_CONTEXT";
  private static final String WATERMARK = "WATERMARK";

  private MockKeyValueStore<KV<String, String>, byte[]> store;
  private KStateInternals<String> stateInternals;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    stateInternals =
        KStateInternals.<String>of(
                NAME, new MockProcessorContext(Collections.singletonMap(NAME, store)))
            .withKey(KEY);
  }

  @Test
  public void testGetKey() {
    Assert.assertEquals(KEY, stateInternals.getKey());
  }

  @Test
  public void testBindValue() {
    Assert.assertEquals(
        KValueState.class,
        stateInternals
            .state(StateNamespaces.global(), StateTags.value(VALUE, VarIntCoder.of()), null)
            .getClass());
  }

  @Test
  public void testBindBag() {
    Assert.assertEquals(
        KBagState.class,
        stateInternals
            .state(StateNamespaces.global(), StateTags.bag(BAG, VarIntCoder.of()), null)
            .getClass());
  }

  @Test
  public void testBindSet() {
    Assert.assertEquals(
        KSetState.class,
        stateInternals
            .state(StateNamespaces.global(), StateTags.set(SET, VarIntCoder.of()), null)
            .getClass());
  }

  @Test
  public void testBindMap() {
    Assert.assertEquals(
        KMapState.class,
        stateInternals
            .state(
                StateNamespaces.global(),
                StateTags.map(MAP, StringUtf8Coder.of(), VarIntCoder.of()),
                null)
            .getClass());
  }

  @Test
  public void testBindOrderedList() {
    Assert.assertEquals(
        KOrderedListState.class,
        stateInternals
            .state(
                StateNamespaces.global(),
                StateTags.orderedList(ORDERED_LIST, VarIntCoder.of()),
                null)
            .getClass());
  }

  @Test
  public void testBindCombining() {
    Assert.assertEquals(
        KCombiningState.class,
        stateInternals
            .state(
                StateNamespaces.global(),
                StateTags.combiningValue(COMBINING, VarIntCoder.of(), null),
                null)
            .getClass());
  }

  @Test
  public void testBindCombiningWithContext() {
    Assert.assertEquals(
        KCombiningWithContextState.class,
        stateInternals
            .state(
                StateNamespaces.global(),
                StateTags.combiningValueWithContext(COMBINING_WITH_CONTEXT, VarIntCoder.of(), null),
                null)
            .getClass());
  }

  @Test
  public void testBindWatermarkHold() {
    Assert.assertEquals(
        KWatermarkHoldState.class,
        stateInternals
            .state(
                StateNamespaces.global(), StateTags.watermarkStateInternal(WATERMARK, null), null)
            .getClass());
  }
}
