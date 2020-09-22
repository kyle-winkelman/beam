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
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link org.apache.beam.runners.kafka.streams.state.KValueState}. */
public class KValueStateTest {

  private static final String KEY = "KEY";
  private static final String ID = "ID";
  private static final Integer VALUE = 1;

  private MockKeyValueStore<KV<String, String>, byte[]> store;
  private KValueState<String, Integer> valueState;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    valueState =
        new KValueState<String, Integer>(
            KEY, StateNamespaces.global(), ID, store, VarIntCoder.of());
  }

  @Test
  public void testRead() {
    valueState.write(VALUE);
    Assert.assertEquals(VALUE, valueState.readLater().read());
    valueState.clear();
    Assert.assertNull(valueState.readLater().read());
  }
}
