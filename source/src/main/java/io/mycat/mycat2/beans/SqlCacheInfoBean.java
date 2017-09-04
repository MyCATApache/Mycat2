package io.mycat.mycat2.beans;

/**
 * 进行SQL缓存的bean的信息
 * 
 * @since 2017年9月4日 下午2:06:04
 * @version 0.0.1
 * @author liujun
 */
public class SqlCacheInfoBean {

	/**
	 * 缓存的内存地址信息
	 */
	private long memoryAddress;


	/**
	 * 内存缓存的大小
	 */
	private int cacheSize;

	/**
	 * 写入文件的位置
	 */
	private int putOption;

	public long getMemoryAddress() {
		return memoryAddress;
	}

	public void setMemoryAddress(long memoryAddress) {
		this.memoryAddress = memoryAddress;
	}


	public int getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

	public int getPutOption() {
		return putOption;
	}

	public void setPutOption(int putOption) {
		this.putOption = putOption;
	}

}
