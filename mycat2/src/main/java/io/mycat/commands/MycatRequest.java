/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.commands;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;


@Getter
@ToString
@Builder
/**
 * @author Junwen Chen
 **/
public class MycatRequest {
    final int sessionId;
    final String text;
    final Map<String,Object> context;
    public MycatRequest(int sessionId, String text, Map<String, Object> context) {
        this.sessionId = sessionId;
        this.text = text;
        this.context = context;
    }
    public <T> T get(String key) {
        return (T)context.get(key);
    }
    public <T> T getOrDefault(String key, String defaultValue) {
        return (T)context.getOrDefault(key,defaultValue);
    }

}