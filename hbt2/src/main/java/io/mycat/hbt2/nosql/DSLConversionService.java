package io.mycat.hbt2.nosql;

/**
 * nosql转换服务
 * dsl转sql 或 sql转dsl
 * @author wangzihaogithub
 * 2020年6月9日 00:57:05
 */
public interface DSLConversionService {
    NosqlDSL convert(String sql);
    String convert(NosqlDSL dsl);
}
