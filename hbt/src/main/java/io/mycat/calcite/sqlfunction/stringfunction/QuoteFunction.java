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
package io.mycat.calcite.sqlfunction.stringfunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;


public class QuoteFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(QuoteFunction.class,
            "quote");

    public static final QuoteFunction INSTANCE = new QuoteFunction();

    public QuoteFunction() {
        super("QUOTE", scalarFunction);
    }

    public static String quote(String name) {
        if (name == null || name.length() == 0) {
            return name;
        }

        char firstChar = name.charAt(0);
        if (firstChar >= 48 && firstChar <= 57) {
            return name = "\"" + name + "\"";
        }

        if (name.length() > 2 && firstChar == '`' && name.charAt(name.length() - 1) == '`') {
            String str = name.substring(1, name.length() -1);
            str = str.replaceAll("\\\"", "\\\"\\\"");
            return '"' + str + '"';
        } else if (name.length() > 2 && firstChar == '\'' && name.charAt(name.length() - 1) == '\'') {
            if (name.indexOf('"') != -1) {
                String str = name.substring(1, name.length() -1);
                str = str.replaceAll("\\\"", "\\\"\\\"");
                return '"' + str + '"';
            } else {
                char[] chars = name.toCharArray();
                chars[0] = '"';
                chars[chars.length - 1] = '"';
                return new String(chars);
            }
        } else if (name.length() == 2 && firstChar == '\'' && name.charAt(1) == '\'') {
            return "\"\"";
        } else if (name.length() > 0 && firstChar != '"') {
            for (int i = 0; i < name.length(); ++i) {
                boolean unicode = false;
                char ch = name.charAt(i);
                if (ch > 128) {
                    unicode = true;
                }
                if (unicode) {
                    String name2 = '"' + name + '"';
                    return name2;
                }
            }
        }

        return name;
    }

}