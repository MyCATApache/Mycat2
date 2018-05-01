package io.mycat.mycat2;


/**
 * Created by zagnix on 2016/7/6.
 */

import java.nio.ByteBuffer;

/**
 * 一行数据是从哪个节点来的。
 * 通过dataNode查找对应的sorter，
 * 将数据放到对应的datanode的sorter，
 * 进行排序.
 */
public final class PackWraper {
    public ByteBuffer rowData;
    public String dataNode;

    public PackWraper(ByteBuffer rowData, String dataNode) {
        this.rowData = rowData;
        this.dataNode = dataNode;
    }
}
