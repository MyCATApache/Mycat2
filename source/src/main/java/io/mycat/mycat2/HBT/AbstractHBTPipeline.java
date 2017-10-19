package io.mycat.mycat2.HBT;

import java.util.List;

/*sql返回的处理  
 * begin 初始化
 * onHeader 处理字段名称
 * onRowData 数据处理
 * */
public interface AbstractHBTPipeline {
	default public void begin (int i) {};
	public ResultSetMeta onHeader (ResultSetMeta header) ;
	public List<byte[]> onRowData(List<byte[]> row) ;
	public void onEnd() ;
	default public void onError(String msg) { };
}
