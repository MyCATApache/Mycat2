package io.mycat.mycat2.beans;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.Semaphore;

/**
 * 实例内存文件映射的缓存
 * 
 * @since 2017年9月4日 下午7:02:52
 * @version 0.0.1
 * @author liujun
 */
public class SqlCacheMapFileBean extends SqlCacheInfoBean {

	/**
	 * 文件名称 fileName
	 */
	private String fileName;

	/**
	 * 随机文件读写信息 randomFile
	 */
	private RandomAccessFile randomFile;

	/**
	 * 文件通道信息 channel
	 */
	private FileChannel channel;
	
	
	/**
	 * 栅栏，用于控制并发的访问数
	 */
	private Semaphore semap = new Semaphore(1);
	

	public RandomAccessFile getRandomFile() {
		return randomFile;
	}

	public void setRandomFile(RandomAccessFile randomFile) {
		this.randomFile = randomFile;
	}

	public FileChannel getChannel() {
		return channel;
	}

	public void setChannel(FileChannel channel) {
		this.channel = channel;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Semaphore getSemap() {
		return semap;
	}

	public void setSemap(Semaphore semap) {
		this.semap = semap;
	}
	

}
