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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.kafka.streams.processor.StateStore;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link org.apache.beam.runners.kafka.streams.state.KStateInternals}. */
public class KTimerInternalsTest {

  private static final String NAME = "NAME";
  private static final String KEY = "KEY";
  private static final String ID_FAMILY = "ID_FAMILY";
  private static final String ID_ONE = "ID_ONE";
  private static final String ID_TWO = "ID_TWO";
  private static final String ID_THREE = "ID_THREE";
  private static final String ID_FOUR = "ID_FOUR";
  private static final Instant INSTANT = Instant.now();

  private MockKeyValueStore<String, List<TimerData>> store;
  private MockKeyValueStore<String, Map<String, Instant>> watermarks;
  private KTimerInternals<String> timerInternals;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    watermarks = new MockKeyValueStore<>();
    Map<String, StateStore> stores = new HashMap<>();
    stores.put(NAME, store);
    stores.put("watermark", watermarks);
    timerInternals =
        KTimerInternals.<String>of(NAME, new MockProcessorContext(stores)).withKey(KEY);
  }

  @Test
  public void testSetAndGetFireableTimers() {
    Instant future = INSTANT.plus(1);
    timerInternals.setTimer(
        StateNamespaces.global(), ID_ONE, ID_FAMILY, INSTANT, INSTANT, TimeDomain.EVENT_TIME);
    timerInternals.setTimer(
        StateNamespaces.global(), ID_TWO, ID_FAMILY, INSTANT, INSTANT, TimeDomain.PROCESSING_TIME);
    timerInternals.setTimer(
        StateNamespaces.global(),
        ID_THREE,
        ID_FAMILY,
        INSTANT,
        INSTANT,
        TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    timerInternals.setTimer(
        StateNamespaces.global(),
        ID_FOUR,
        ID_FAMILY,
        future,
        future,
        TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    timerInternals.advanceInputWatermarkTime(Collections.emptySet());
    timerInternals.advanceOutputWatermarkTime(future);
    timerInternals.advanceProcessingTime(future);
    timerInternals.advanceSynchronizedProcessingTime(future);
    timerInternals.fireTimers(
        (key, timers) -> {
          Assert.assertEquals(KEY, key);
          Assert.assertEquals(3, timers.size());
          for (TimerData timer : timers) {
            Assert.assertEquals(StateNamespaces.global(), timer.getNamespace());
            Assert.assertEquals(ID_FAMILY, timer.getTimerFamilyId());
            Assert.assertEquals(INSTANT, timer.getTimestamp());
            Assert.assertEquals(INSTANT, timer.getOutputTimestamp());
            switch (timer.getTimerId()) {
              case ID_ONE:
                Assert.assertEquals(TimeDomain.EVENT_TIME, timer.getDomain());
                break;
              case ID_TWO:
                Assert.assertEquals(TimeDomain.PROCESSING_TIME, timer.getDomain());
                break;
              case ID_THREE:
                Assert.assertEquals(TimeDomain.SYNCHRONIZED_PROCESSING_TIME, timer.getDomain());
                break;
              default:
                Assert.fail("Unknown timerId.");
            }
          }
        });

    Assert.assertEquals(1, store.map.size());
  }

  @Test
  public void testDelete() {
    timerInternals.setTimer(
        StateNamespaces.global(), ID_ONE, ID_FAMILY, INSTANT, INSTANT, TimeDomain.EVENT_TIME);
    timerInternals.deleteTimer(StateNamespaces.global(), ID_ONE, TimeDomain.EVENT_TIME);
    Assert.assertEquals(0, store.map.size());
  }
}
