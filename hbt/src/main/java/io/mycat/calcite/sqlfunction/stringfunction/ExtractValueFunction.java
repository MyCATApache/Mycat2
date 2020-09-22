package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
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

public class ExtractValueFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ExtractValueFunction.class,
            "extractValue");
    public static final ExtractValueFunction INSTANCE = new ExtractValueFunction();

    public ExtractValueFunction() {
        super(new SqlIdentifier("EXTRACTVALUE", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),
                InferTypes.FIRST_KNOWN,
                OperandTypes.VARIADIC,
                ImmutableList.of(), scalarFunction);
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