package io.mycat.mycat2.cmds.cache.mapcache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.beans.SqlCacheInfoBean;
import io.mycat.mycat2.beans.SqlCacheMapFileBean;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.IOUtils;
import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

/**
 * 文件映射的缓存的实现
 * 
 * @since 2017年9月4日 下午1:56:07
 * @version 0.0.1
 * @author liujun
 */
@SuppressWarnings("restriction")
public class MapFileCacheImp implements CacheInf<SqlCacheMapFileBean> {

	/**
	 * 内存文件映射的实体对象
	 */
	public static final MapFileCacheImp INSTANCE = new MapFileCacheImp();

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(MapFileCacheImp.class);

	/**
	 * 内存控制的对象信息 unsafe
	 */

	public static Unsafe unsafe;

	/**
	 * 获得内存映射的方法 mmap
	 */
	public static final Method mmap;

	/**
	 * 解除映射的方法 unmmap
	 */
	public static final Method unmmap;

	/**
	 * 用于计算的byte字节数
	 */
	private static final int COUNT_SIZE = 20;

	/**
	 * 最大文件数
	 */
	private static final int MAX_DIRECTORY = 100;

	static {
		try {
			Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
			singleoneInstanceField.setAccessible(true);
			unsafe = (Unsafe) singleoneInstanceField.get(null);
			mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
			mmap.setAccessible(true);
			unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
			unmmap.setAccessible(true);
		} catch (Exception e) {
			logger.error("MapFileCacheImp init Unsafe object error", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * 获取map影射文件信息
	 * 
	 * 同步方法，同一个SQL的缓存只存在一个
	 * 
	 * @param buffer
	 *            需要写入缓存文件数据信息
	 * @param size
	 *            内存块的大小
	 * @return 缓存的文件信息
	 * @throws IOException
	 *             可能的异常信息
	 */
	public SqlCacheMapFileBean createCacheFile(byte[] buffer, int size) throws IOException, InterruptedException {

		SqlCacheMapFileBean sqlCahce = new SqlCacheMapFileBean();

		String path = MapFileCacheImp.class.getClassLoader().getResource("cachefile").getPath();

		// 取20字符进行byte相加，计算终最的目录
		if (null != buffer && buffer.length > COUNT_SIZE) {
			int sumValue = 0;
			for (int i = 0; i < COUNT_SIZE; i++) {
				sumValue += buffer[i];
			}
			// 对结果进行取模求得最终目录
			int outDirectory = sumValue % MAX_DIRECTORY;

			path = path + File.separator + outDirectory;

			// 检查 文件是否存在
			File checkExists = new File(path);

			if (!checkExists.exists()) {
				checkExists.mkdirs();
			}
		}
		// 如果不到20个字节，则使用随机数
		else {
			int outDirectory = ThreadLocalRandom.current().nextInt(MAX_DIRECTORY);

			path = path + File.separator + outDirectory;

			// 检查 文件是否存在
			File checkExists = new File(path);

			if (!checkExists.exists()) {
				checkExists.mkdirs();
			}
		}

		sqlCahce.setFileName(path + File.separator + "mapFile-" + System.nanoTime() + ".mapfile");

		sqlCahce.setRandomFile(new RandomAccessFile(sqlCahce.getFileName(), "rw"));

		sqlCahce.setChannel(sqlCahce.getRandomFile().getChannel());

		sqlCahce.setCacheSize(size);

		// 初始大小与内存大小相同
		sqlCahce.setMapPosition(size);

		// 进行内存的映射
		mmapMapFile(sqlCahce, 0, sqlCahce.getCacheSize());

		return sqlCahce;
	}

	/**
	 * 进行内存文件的映射操作
	 * 
	 * @param sqlCahce
	 *            缓存的信息
	 * @param offset
	 *            偏移便利店
	 * @param size
	 *            大小
	 * @throws IOException
	 */
	private void mmapMapFile(SqlCacheMapFileBean sqlCahce, long offset, long size) throws IOException {
		try {
			// 获得内存映射的地地址
			sqlCahce.setMemoryAddress((long) mmap.invoke(sqlCahce.getRandomFile().getChannel(), 1, offset, size));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			logger.error("MapFileCacheImp getMapCacheFile error", e);
			throw new IOException(e);
		}
	}

	public static long getPagetSize() {
		return unsafe.pageSize();
	}

	/**
	 * 获取写入的索引编号 方法描述
	 * 
	 * @param offset
	 * @return 2016年12月24日
	 */
	private long getIndex(SqlCacheInfoBean cacheResult, long offset) {
		return cacheResult.getMemoryAddress() + offset;
	}

	/**
	 * 将添加的指针加1
	 * 
	 * @return 2016年12月23日
	 */
	private long addPutPos(SqlCacheInfoBean cacheResult) {
		cacheResult.setPutOption(cacheResult.getPutOption() + 1);
		return cacheResult.getPutOption();
	}

	/**
	 * 获取读取的指针信息
	 * 
	 * @return 2016年12月23日
	 */
	private long getPutPos(SqlCacheInfoBean cacheResult) {
		return cacheResult.getPutOption();
	}

	public void putCacheData(ProxyBuffer proBuffer, SqlCacheMapFileBean cacheResult) throws Exception {

		// 进行加锁操作
		boolean lock = false;

		// 获取文件的游标
		long filePosition = 0;
		try {
			cacheResult.getSemap().acquire();

			lock = true;
			filePosition = (long) cacheResult.getChannel().position();

			ByteBuffer buffer = proBuffer.getBuffer();
			buffer.limit(proBuffer.readIndex);
			buffer.position(proBuffer.readMark);

			long writeLength = filePosition + buffer.limit() - buffer.position();

			// 仅在大于内存映射区域后进行文件大小的设置
			if (writeLength > cacheResult.getMapPosition()) {
				// 进行文件的扩容
				cacheResult.getRandomFile().setLength(writeLength);
				// 重新建立映射通道
				this.unmap(cacheResult);
				// 内存通道映射加大
				this.mmapMapFile(cacheResult, 0, writeLength);
				// 初始大小与内存大小相同
				cacheResult.setMapPosition(writeLength);
			}

			long currPostision = 0;

			// System.out.println("buffer写入大小:" + (buffer.limit() -
			// buffer.position()));
			// 将数据写入到内存的映射中
			for (int i = buffer.position(); i < buffer.limit(); i++) {
				currPostision = getPutPos(cacheResult);
				// 进行内存数据写入
				unsafe.putByte(cacheResult.getMemoryAddress() + currPostision, buffer.get(i));

				currPostision = addPutPos(cacheResult);
			}

			// 设置新的位置
			cacheResult.getChannel().position(writeLength);

		} catch (IOException e) {
			logger.error("MapFileCacheImp putCacheData IOException", e);
			e.printStackTrace();
			throw e;
		} finally {
			// 仅当加锁成功后才释放许可
			if (lock) {
				// 释放加锁的许可
				cacheResult.getSemap().release();
			}
		}

	}

	public int getByte(ProxyBuffer proBuffer, SqlCacheMapFileBean cacheResult, int offset) throws IOException {

		int length = cacheResult.getPutOption();

		// 如果当前领移的数据超过了大小，则不处理
		if (offset > length) {
			return -1;
		}

		ByteBuffer valueBuff = proBuffer.getBuffer();

		// 计算空间
		int limit = 0;

		// 标识出上一次写入的位置
		proBuffer.readMark = proBuffer.writeIndex;

		if (limit == proBuffer.readMark) {
			limit = valueBuff.limit();
		} else {
			limit = valueBuff.limit() - proBuffer.readMark;
		}

		int endPosition = 0;

		// 检查结果集是否需要多次的返回
		if (limit > length) {
			for (int i = 0; i < length; i++) {
				proBuffer.writeByte(unsafe.getByte(getIndex(cacheResult, i)));
			}
			endPosition = offset + length;
		} else {
			int startPosition = offset;
			endPosition = offset;
			// 如果最后一次数据，则以最长的标识为准备，不能超过
			if (offset + limit >= length) {
				endPosition = length;
			}
			// 中间的数据按依稀进行读取
			else {
				endPosition = offset + limit;
			}

			System.out.println("错误的写入位置index:" + startPosition + "buffer信息为:" + proBuffer);

			// 进行数据填充
			for (int i = startPosition; i < endPosition; i++) {
				if (proBuffer.writeIndex >= proBuffer.getBuffer().limit()) {
					System.out.println("当前写入index:" + proBuffer.writeIndex + ",limit:" + proBuffer.getBuffer().limit()
							+ ",当前i:" + i + "，结束位置:<" + endPosition);
				}

				proBuffer.writeByte(unsafe.getByte(getIndex(cacheResult, i)));
			}
		}
		return endPosition;
	}

	private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
		Method m = cls.getDeclaredMethod(name, params);
		m.setAccessible(true);
		return m;
	}

	private void unmap(SqlCacheMapFileBean cacheInfo) throws Exception {
		unmmap.invoke(null, cacheInfo.getMemoryAddress(), cacheInfo.getPutOption());
	}

	public void close(SqlCacheMapFileBean cacheInfo) {

		if (null != cacheInfo) {

			// 关闭流将文件刷入磁盘中
			IOUtils.colse(cacheInfo.getChannel());
			IOUtils.colse(cacheInfo.getRandomFile());

			// 将内存释放
			try {
				this.unmap(cacheInfo);
			} catch (Exception e) {
				logger.error("MapFileCacheImp close unmap Exception", e);
				e.printStackTrace();
			}

			// 缓存结果完成后，需要删除文件
			new File(cacheInfo.getFileName()).delete();

		}
	}

	/**
	 * 进行清理数据
	 * 
	 * @param cacheResult
	 * @return 新的文件映射对象信息
	 * @throws Exception
	 */
	public SqlCacheMapFileBean clean(SqlCacheMapFileBean cacheResult) throws Exception {

		SqlCacheMapFileBean result = null;

		if (null != cacheResult) {
			// 进行加锁操作
			boolean lock = false;

			try {
				cacheResult.getSemap().acquire();
				lock = true;

				// // 重新标识出通道的大小
				// cacheResult.getChannel().position(cacheResult.getCacheSize());
				// // 进行缓存 数据重置写入
				// cacheResult.setPutOption(0);
				// // 重新建立映射通道
				// this.unmap(cacheResult);
				// // 内存通道映射加大
				// this.mmapMapFile(cacheResult, 0, cacheResult.getCacheSize());

				close(cacheResult);
				// 重新创建文件缓存
				result = this.createCacheFile(null, cacheResult.getCacheSize());

			} catch (Exception e) {
				logger.error("MapFileCacheImp clean IOException", e);
				e.printStackTrace();
				throw e;
			} finally {
				// 仅当加锁成功后才释放许可
				if (lock) {
					// 释放加锁的许可
					cacheResult.getSemap().release();
				}
			}
		}

		return result;
	}

}
