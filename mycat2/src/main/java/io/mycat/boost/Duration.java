/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.mycat.boost;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

@Getter
@AllArgsConstructor
public class Duration {
    private long duration;
    private TimeUnit timeUnit;

    public static Duration parse(String key, String value) {
        checkArgument(value != null && !value.isEmpty(), "value of key %s omitted", key);
        long duration;
        TimeUnit timeUnit;
        try {
            char lastChar = value.charAt(value.length() - 1);
            switch (lastChar) {
                case 'd':
                    timeUnit = TimeUnit.DAYS;
                    break;
                case 'h':
                    timeUnit = TimeUnit.HOURS;
                    break;
                case 'm':
                    timeUnit = TimeUnit.MINUTES;
                    break;
                case 's':
                    timeUnit = TimeUnit.SECONDS;
                    break;
                default:
                    throw new IllegalArgumentException(
                            format(
                                    "key %s invalid format.  was %s, must end with one of [dDhHmMsS]", key, value));
            }

            return new Duration(Long.parseLong(value.substring(0, value.length() - 1)), timeUnit);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    format("key %s value set to %s, must be integer", key, value));
        }
    }

    public long toMillis() {
        return timeUnit.toMillis(duration);
    }
}