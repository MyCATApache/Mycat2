package io.mycat.calcite.sqlfunction.stringfunction;

import lombok.SneakyThrows;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.dom4j.tree.AbstractElement;

import java.io.StringReader;
import java.util.List;

public class UpdateXMLFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UpdateXMLFunction.class,
            "updateXML");

    public static final UpdateXMLFunction INSTANCE = new UpdateXMLFunction();

    public UpdateXMLFunction() {
        super("UPDATEXML", scalarFunction);
    }

    @SneakyThrows
    public static String updateXML(
            String xml_target, String xpath_expr, String new_xml) {
        if (xml_target == null || xpath_expr == null || new_xml == null) {
            return null;
        }
        SAXReader reader = new SAXReader();
        Document document = reader.read(new StringReader(xml_target));
        List<Node> ret = document.selectNodes(xpath_expr);
        if (ret == null || ret.size() != 1) {
            return xml_target;
        }
        AbstractElement parent = (AbstractElement)ret.get(0).getParent();
        if (parent == null){
            return new_xml;
        }else {
            Node t = ret.get(0);

            Element replace = reader.read(new StringReader(new_xml)).getRootElement();
            replace.setName(t.getName());
            Element parent1 = t.getParent();
            parent1.remove(t);
            parent1.add(replace);
            return document.getRootElement().asXML();
        }
    }
}