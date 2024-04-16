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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

  public static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  public static final DateFormat SHORT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
  public static final DateFormat DEFAULT_DATETIME_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static String today() {
    return DEFAULT_DATE_FORMAT.format(new Date());
  }

  public static String todayByShort() {
    return SHORT_DATE_FORMAT.format(new Date());
  }

  public static String currentTimeString() {
    return DEFAULT_DATETIME_FORMAT.format(new Date());
  }

  public static String todayBeforeByShort(int days) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -days);
    return SHORT_DATE_FORMAT.format(cal.getTime());
  }

  public static String timestampToDateByShort(long timestamp) {
    return SHORT_DATE_FORMAT.format(new Date(timestamp));
  }
}
