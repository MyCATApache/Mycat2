/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mycat.proxy.buffer;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class Platform {
    
    private static final long MAX_DIRECT_MEMORY;

    static {
        MAX_DIRECT_MEMORY = maxDirectMemory();
    }


    @SuppressWarnings("unchecked")
	private static ClassLoader getSystemClassLoader() {
        return System.getSecurityManager() == null ? ClassLoader.getSystemClassLoader() : (ClassLoader) AccessController.doPrivileged(new PrivilegedAction() {
            public ClassLoader run() {
                return ClassLoader.getSystemClassLoader();
            }
        });
    }

    /**
     * GET  MaxDirectMemory Size
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private static long maxDirectMemory() {
        long maxDirectMemory = 0L;
        Class t;
        
        try {
            t = Class.forName("sun.misc.VM", true, getSystemClassLoader());
            Method runtimeClass = t.getDeclaredMethod("maxDirectMemory", new Class[0]);
            maxDirectMemory = ((Number) runtimeClass.invoke((Object) null, new Object[0])).longValue();
        } catch (Throwable var8) {
            ;
        }
        return maxDirectMemory;
    }

    public static long getMaxDirectMemory() {
        return MAX_DIRECT_MEMORY;
    }
}
