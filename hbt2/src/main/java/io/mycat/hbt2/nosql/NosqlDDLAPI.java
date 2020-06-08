package io.mycat.hbt2.nosql;

/**
 * nosql修改接口
 * @author wangzihaogithub
 * 2020年6月9日 00:57:05
 */
public interface NosqlDDLAPI {
    BulkResponse<InsertResponse> insertBulk(NosqlDSL dsl);
    InsertResponse insert(NosqlDSL dsl);

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
