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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.kafka.streams.watermark.Watermark;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.Instant;

public class KTimerInternals<K> implements TimerInternals {

  public static final String WATERMARK = "__watermark";

  @SuppressWarnings("unchecked")
  public static <K> KTimerInternals<K> of(
      String timerStoreName, ProcessorContext processorContext) {
    System.out.println("KTimerInternals timerStoreName: " + timerStoreName);
    return new KTimerInternals<>(
        (KeyValueStore<K, Map<String, TimerData>>) processorContext.getStateStore(timerStoreName));
  }

  private final KeyValueStore<K, Map<String, TimerData>> store;

  private Instant processingTime;
  private Instant synchronizedProcessingTime;
  private Instant inputWatermarkTime;
  private Instant outputWatermarkTime;
  private K key;

  public KTimerInternals(KeyValueStore<K, Map<String, TimerData>> store) {
    this.store = store;
    this.processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.outputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  public KTimerInternals<K> withKey(K key) {
    this.key = key;
    return this;
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant target,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    setTimer(TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain));
  }

  @Override
  public void setTimer(TimerData timerData) {
    Map<String, TimerData> timers = store.get(key);
    if (timers == null) {
      timers = new HashMap<>();
    }
    timers.put(id(timerData), timerData);
    store.put(key, timers);
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, @Nullable TimeDomain timeDomain) {
    Map<String, TimerData> timers = store.get(key);
    if (timers == null) {
      return;
    }
    timers.remove(id(namespace, timerId));
    if (timers.isEmpty()) {
      store.delete(key);
    } else {
      store.put(key, timers);
    }
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    deleteTimer(namespace, timerId, (TimeDomain) null);
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    deleteTimer(timerKey.getNamespace(), timerKey.getTimerId(), timerKey.getDomain());
  }

  public void watermark(Watermark watermark) {
    Map<String, TimerData> watermarks = store.get(null);
    if (watermarks == null) {
      watermarks = new HashMap<>();
    }
    watermarks.put(
        new String(watermark.id().get(), StandardCharsets.UTF_8),
        TimerData.of(
            WATERMARK,
            WATERMARK,
            StateNamespaces.global(),
            watermark.watermark(),
            watermark.watermark(),
            TimeDomain.EVENT_TIME));
    store.put(null, watermarks);
  }

  public void advanceProcessingTime(Instant processingTime) {
    this.processingTime = processingTime;
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  public void advanceSynchronizedProcessingTime(Instant synchronizedProcessingTime) {
    this.synchronizedProcessingTime = synchronizedProcessingTime;
  }

  @Override
  public Instant currentSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  public void advanceInputWatermarkTime() {
    Map<String, TimerData> watermarks = store.get(null);
    if (watermarks == null) {
      System.out.println("advanceInputWatermarkTime watermarks null");
      return;
    }
    watermarks = Maps.filterEntries(watermarks, e -> e.getValue().getTimerId().equals(WATERMARK));
    Instant inputWatermarkTime = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (TimerData watermark : watermarks.values()) {
      System.out.println("advanceInputWatermarkTime timerData: " + watermark);
      if (watermark.getTimestamp().isBefore(inputWatermarkTime)) {
        inputWatermarkTime = watermark.getTimestamp();
      }
    }
    if (inputWatermarkTime.isAfter(this.inputWatermarkTime)) {
      this.inputWatermarkTime = inputWatermarkTime;
    }
    System.out.println("advanceInputWatermarkTime: " + this.inputWatermarkTime);
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  public void advanceOutputWatermarkTime(Instant outputWatermarkTime) {
    this.outputWatermarkTime = outputWatermarkTime;
  }

  @Override
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  public void fireTimers(BiConsumer<K, List<TimerData>> consumer) {
    boolean hasFired;
    do {
      hasFired = false;
      Iterator<KeyValue<K, Map<String, TimerData>>> iterator = store.all();
      while (iterator.hasNext()) {
        KeyValue<K, Map<String, TimerData>> keyValue = iterator.next();
        List<TimerData> fireableTimers =
            keyValue.value.entrySet().stream()
                .map(Map.Entry::getValue)
                .filter(timerData -> !timerData.getTimerId().equals(KTimerInternals.WATERMARK))
                .filter(
                    timerData ->
                        domainTime(timerData).isAfter(timerData.getTimestamp())
                            || domainTime(timerData).isEqual(timerData.getTimestamp()))
                .collect(Collectors.toList());
        if (!fireableTimers.isEmpty()) {
          fireableTimers.forEach(this::deleteTimer);
          consumer.accept(keyValue.key, fireableTimers);
          hasFired = true;
        }
      }
    } while (hasFired);
  }

  private Instant domainTime(TimerData timerData) {
    switch (timerData.getDomain()) {
      case EVENT_TIME:
        return currentInputWatermarkTime();
      case PROCESSING_TIME:
        return currentProcessingTime();
      case SYNCHRONIZED_PROCESSING_TIME:
        return currentSynchronizedProcessingTime();
      default:
        throw new IllegalArgumentException("unknown timeDomain: " + timerData.getDomain());
    }
  }

  private String id(TimerData timerData) {
    return id(timerData.getNamespace(), timerData.getTimerId());
  }

  private String id(StateNamespace namespace, String timerId) {
    return namespace.stringKey() + "+" + timerId;
  }
}
