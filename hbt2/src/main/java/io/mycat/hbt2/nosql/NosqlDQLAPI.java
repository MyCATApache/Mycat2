package io.mycat.hbt2.nosql;

import io.mycat.util.Pair;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * nosql查询接口
 * @author wangzihaogithub
 * 2020年6月9日 00:57:05
 */
public interface NosqlDQLAPI {
    RowDQLResponse queryRow(NosqlDSL dsl);
    PairDQLResponse queryPair(NosqlDSL dsl);

    interface DQLResponse extends NosqlResponse{
    }

    /**
     * 行格式的元数据
     */
    interface RowMetaData{
        /**
         * columnIndex从1开始, 与jdbc保持逻辑一致
         * @param columnIndex 列名
         * @return 列名
         */
        String getColumnName(int columnIndex);
        int getColumnCount();
    }
    /**
     * 行格式响应返回 (表格)
     */
    interface RowDQLResponse extends DQLResponse,Iterator<NosqlValue[]>{
        RowMetaData getMetaData();
    }
    /**
     * 列格式响应返回 (键值对)
     */
    interface PairDQLResponse extends DQLResponse,Iterable<Pair<String,NosqlValue>> {
    }

}
