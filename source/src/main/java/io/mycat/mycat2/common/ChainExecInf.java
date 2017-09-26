package io.mycat.mycat2.common;

/**
 * 责任链业务接口定义
 * 
 * @author liujun
 *
 * @date 2014年4月8日
 * @vsersion 0.0.1
 */
public interface ChainExecInf {

	/**
	 * 进行正常流程执行的代码
	 * 
	 * @param seq
	 * @return
	 * @throws Exception
	 */
	public boolean invoke(SeqContextList seqList) throws Exception;

}
