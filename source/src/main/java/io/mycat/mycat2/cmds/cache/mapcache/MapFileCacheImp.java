package io.mycat.mycat2.cmds.cache.mapcache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.beans.SqlCacheMapFileBean;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.IOUtils;

/**
 * 文件映射的缓存的实现
 * 
 * @since 2017年9月4日 下午1:56:07
 * @version 0.0.1
 * @author liujun
 */
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
	 * 用于计算的byte字节数
	 */
	private static final int COUNT_SIZE = 20;

	/**
	 * 最大文件数
	 */
	private static final int MAX_DIRECTORY = 100;

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

		// 设置通道信息
		sqlCahce.setChannel(sqlCahce.getRandomFile().getChannel());

		sqlCahce.setCacheSize(size);

		// 初始大小与内存大小相同
		sqlCahce.setMapPosition(size);

		return sqlCahce;
	}

	public void putCacheData(ProxyBuffer proBuffer, SqlCacheMapFileBean cacheResult) throws Exception {

		// 进行加锁操作
		boolean lock = false;

		try {
			cacheResult.getSemap().acquire();

			lock = true;

			ByteBuffer buffer = proBuffer.getBuffer();
			buffer.limit(proBuffer.readIndex);
			buffer.position(proBuffer.readMark);

			// 写入数据
			cacheResult.getChannel().write(buffer);

			// 设置大小
			cacheResult.setPutOption(cacheResult.getChannel().position());

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

	public long getByte(ProxyBuffer proBuffer, SqlCacheMapFileBean cacheResult, long offset) throws IOException {

		// 进行加锁操作
		boolean lock = false;
		try {
			cacheResult.getSemap().acquire();
			lock = true;

			long length = cacheResult.getPutOption();

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

			long endPosition = 0;

			// 检查结果集是否需要多次的返回
			if (limit > length) {
				cacheResult.getChannel().position(offset);
				cacheResult.getChannel().read(valueBuff);
				endPosition = offset + length;
			} else {
				long startPosition = offset;
				endPosition = offset;
				// 如果最后一次数据，则以最长的标识为准备，不能超过
				if (offset + limit >= length) {
					endPosition = length;
				}
				// 中间的数据按依稀进行读取
				else {
					endPosition = offset + limit;
				}

				cacheResult.getChannel().position(startPosition);

				cacheResult.getChannel().read(valueBuff);
			}
			return endPosition;
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			// 仅当加锁成功后才释放许可
			if (lock) {
				// 释放加锁的许可
				cacheResult.getSemap().release();
			}

		}

		return 0;

	}

	public void close(SqlCacheMapFileBean cacheInfo) {

		if (null != cacheInfo) {

			// 关闭流将文件刷入磁盘中
			IOUtils.colse(cacheInfo.getChannel());
			IOUtils.colse(cacheInfo.getOutputFile());

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

	public static void main(String[] args) {
		ByteBuffer buffer = ByteBuffer.allocate(128);
		for (int i = 0; i < 128; i++) {
			buffer.put((byte) i);
		}

		buffer.position(64);

		int size = buffer.limit() - buffer.position();

		byte[] buffs = new byte[size];

		buffer.get(buffs);

		System.out.println(Arrays.toString(buffs));

	}

}
