package io.mycat.nosql;

import java.util.Map;

/**
 * nosql 领域语言
 * @author wangzihaogithub
 * 2020年6月9日 00:57:35
 */
public interface NosqlDSL extends Map<String,Object> {

    /**
     * 获得nosql提供商的名称
     * @return 名称
     */
    NosqlProvider getProvider();

    /**
     * toString要转成dsl的语句
     * @return dsl语句
     */
    @Override
    String toString();
}
