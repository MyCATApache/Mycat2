package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotation;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by jamie on 2017/9/5.
 */
public class DynamicAnnotationTest {
    public static void main(String[] args) throws Exception{
        Yaml yaml = new Yaml();
        DynamicAnnotation dynamicAnnotation = yaml.loadAs(new FileInputStream(new File("D:\\SQLparserNew\\src\\main\\resources\\dynamicAnnotation.yaml")), DynamicAnnotation.class);
        System.out.println(dynamicAnnotation.toString());
    }
}
