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
package org.apache.beam.runners.kafka.streams;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.kafka.streams.KafkaStreams;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamsPipelineResult implements PipelineResult {

  public static KafkaStreamsPipelineResult of(KafkaStreams kafkaStreams) {
    return new KafkaStreamsPipelineResult(kafkaStreams);
  }

  private final KafkaStreams kafkaStreams;
  private final CountDownLatch countDownLatch;
  private State state;

  private KafkaStreamsPipelineResult(KafkaStreams kafkaStreams) {
    this.kafkaStreams = kafkaStreams;
    this.countDownLatch = new CountDownLatch(1);
    this.kafkaStreams.setStateListener(new StateListener());
    this.kafkaStreams.setUncaughtExceptionHandler(new UncaughtExceptionHandler());
    this.kafkaStreams.cleanUp();
    this.kafkaStreams.start();
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State cancel() throws IOException {
    kafkaStreams.close();
    return getState();
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.ZERO);
  }

  @Nullable
  @Override
  public State waitUntilFinish(Duration duration) {
    long millis = duration.getMillis();
    boolean timeout = false;
    try {
      if (millis < 1) {
        countDownLatch.await();
      } else {
        timeout = countDownLatch.await(millis, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    if (timeout) {
      return null;
    } else {
      return state;
    }
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }

  private class StateListener implements KafkaStreams.StateListener {

    private final Logger log = LoggerFactory.getLogger(StateListener.class);

    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
      log.error("State changed from {} to {}.", oldState, newState);
      state = toState(newState);
      if (state == State.FAILED || state == State.CANCELLED) {
        countDownLatch.countDown();
      }
    }

    private State toState(KafkaStreams.State state) {
      switch (state) {
        case CREATED:
          return State.STOPPED;
        case ERROR:
          return State.FAILED;
        case NOT_RUNNING:
          return State.CANCELLED;
        case PENDING_SHUTDOWN:
          return State.CANCELLED;
        case REBALANCING:
          return State.STOPPED;
        case RUNNING:
          return State.RUNNING;
        default:
          return State.UNKNOWN;
      }
    }
  }

  private static class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(UncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.error("Thread {} threw {}", t, e);
    }
  }
}
