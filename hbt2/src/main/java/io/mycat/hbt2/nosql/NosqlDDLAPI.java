package io.mycat.hbt2.nosql;

/**
 * nosql修改接口
 * @author wangzihaogithub
 * 2020年6月9日 00:57:05
 */
public interface NosqlDDLAPI {
    /**
     * 批量插入
     * @param dsl 领域语音
     * @return 插入结果
     */
    BulkResponse<InsertResponse> insertBulk(NosqlDSL dsl);
    InsertResponse insert(NosqlDSL dsl);

    /**
     * 批量更新
     * @param dsl 领域语音
     * @return 更新结果
     */
    BulkResponse<UpdateResponse> updateBulk(NosqlDSL dsl);
    UpdateResponse update(NosqlDSL dsl);


    interface BulkResponse<E extends DDLResponse> extends Iterable<E> {
    }
    interface DDLResponse extends NosqlResponse{
    }
    interface InsertResponse extends DDLResponse{
    }
    interface UpdateResponse extends DDLResponse{
    }
}
