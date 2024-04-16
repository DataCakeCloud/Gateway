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

package org.apache.kyuubi.util;

import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AutoRetry {

  private static final Logger LOG = LoggerFactory.getLogger(AutoRetry.class);

  private static final long MAX_SLEEP_MILLISECOND = 256 * 1000;

  public static <T> T executeWithRetry(
      Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond, boolean exponential)
      throws Exception {
    Retry retry = new Retry();
    return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
  }

  private static class Retry {

    public <T> T doRetry(
        Callable<T> callable,
        int retryTimes,
        long sleepTimeInMilliSecond,
        boolean exponential,
        List<Class<?>> retryExceptionClasss)
        throws Exception {
      if (null == callable) {
        throw new IllegalArgumentException("InvalidParameters, callable cannot null");
      }

      if (retryTimes < 1) {
        throw new IllegalArgumentException(
            String.format("InvalidParameters, retryTimes: [%d] cannot less than 1", retryTimes));
      }

      Exception saveException = null;
      for (int i = 0; i < retryTimes; i++) {
        try {
          return call(callable);
        } catch (Exception e) {
          saveException = e;
          if (i == 0) {
            LOG.error(
                String.format(
                    "Exception when calling callable, errMsg: %s", saveException.getMessage()),
                saveException);
          }

          if (null != retryExceptionClasss && !retryExceptionClasss.isEmpty()) {
            boolean needRetry = false;
            for (Class<?> eachExceptionClass : retryExceptionClasss) {
              if (eachExceptionClass == e.getClass()) {
                needRetry = true;
                break;
              }
            }
            if (!needRetry) {
              throw saveException;
            }
          }

          if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
            long startTime = System.currentTimeMillis();

            long timeToSleep;
            if (exponential) {
              timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
              if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                timeToSleep = MAX_SLEEP_MILLISECOND;
              }
            } else {
              timeToSleep = sleepTimeInMilliSecond;
              if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                timeToSleep = MAX_SLEEP_MILLISECOND;
              }
            }

            try {
              Thread.sleep(timeToSleep);
            } catch (InterruptedException ignored) {
            }

            long realTimeSleep = System.currentTimeMillis() - startTime;

            LOG.error(
                String.format(
                    "Exception when calling callable, retryTimes: [%s], wait: [%s]ms, realWait: [%s]ms, errMsg:[%s]",
                    i + 1, timeToSleep, realTimeSleep, e.getMessage()));
          }
        }
      }
      throw saveException;
    }

    protected <T> T call(Callable<T> callable) throws Exception {
      return callable.call();
    }
  }
}
