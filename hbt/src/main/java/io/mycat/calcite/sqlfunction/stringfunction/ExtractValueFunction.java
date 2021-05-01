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

import com.google.common.collect.ImmutableList;

import lombok.SneakyThrows;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.StringReader;

public class ExtractValueFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ExtractValueFunction.class,
            "extractValue");
    public static final ExtractValueFunction INSTANCE = new ExtractValueFunction();

    public ExtractValueFunction() {
        super("EXTRACTVALUE", scalarFunction);
    }

    @SneakyThrows
    public static String extractValue(String xml_frag, String xpath_expr) {
        if (xml_frag == null||xpath_expr==null){
            return null;
        }

        if (!xpath_expr.toLowerCase().endsWith("/text()")) {
            xpath_expr += "/text() ";
        }
        SAXReader reader = new SAXReader();
        Document document = reader.read(new StringReader(xml_frag));
        Node ret = document.selectSingleNode(xpath_expr);
        if (ret == null) {
            return null;
        } else {
            return ret.asXML();
        }

    }
}