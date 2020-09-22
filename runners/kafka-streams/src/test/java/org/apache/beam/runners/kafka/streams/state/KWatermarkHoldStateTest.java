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

import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link org.apache.beam.runners.kafka.streams.state.KWatermarkHoldState}. */
public class KWatermarkHoldStateTest {

  private static final String KEY = "KEY";
  private static final String ID = "ID";
  private static final Instant WATERMARK_ONE = Instant.now();
  private static final Instant WATERMARK_TWO = Instant.now();

  private MockKeyValueStore<KV<String, String>, byte[]> store;
  private KWatermarkHoldState<String> watermarkHoldeState;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    watermarkHoldeState =
        new KWatermarkHoldState<String>(
            KEY, StateNamespaces.global(), ID, store, TimestampCombiner.LATEST);
  }

  @Test
  public void testAddFirstAndAdditionalThenRead() {
    watermarkHoldeState.add(WATERMARK_ONE);
    watermarkHoldeState.add(WATERMARK_TWO);
    Assert.assertEquals(WATERMARK_TWO, watermarkHoldeState.readLater().read());
  }

  @Test
  public void testCheckIsEmptyBeforeAndAfterClear() {
    watermarkHoldeState.add(WATERMARK_ONE);
    Assert.assertFalse(watermarkHoldeState.isEmpty().readLater().read());
    watermarkHoldeState.clear();
    Assert.assertTrue(watermarkHoldeState.isEmpty().readLater().read());
  }

  @Test
  public void testGetTimestampCombiner() {
    Assert.assertEquals(TimestampCombiner.LATEST, watermarkHoldeState.getTimestampCombiner());
  }
}
