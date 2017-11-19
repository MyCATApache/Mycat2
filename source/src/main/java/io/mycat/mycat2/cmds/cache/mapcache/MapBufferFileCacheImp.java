package io.mycat.mycat2.cmds.cache.mapcache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.beans.SqlCacheInfoBean;
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
public class MapBufferFileCacheImp implements CacheInf<SqlCacheMapFileBean> {

	/**
	 * 内存文件映射的实体对象
	 */
	public static final MapBufferFileCacheImp INSTANCE = new MapBufferFileCacheImp();

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(MapBufferFileCacheImp.class);

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

		String path = MapBufferFileCacheImp.class.getClassLoader().getResource("cachefile").getPath();

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

		// 初始化通道都为0
		sqlCahce.getChannel().position(0);

		// 设置文件内存映射
		sqlCahce.setMappedBuffer(sqlCahce.getRandomFile().getChannel().map(MapMode.READ_WRITE, 0, size));

		sqlCahce.setCacheSize(size);

		// 初始大小与内存大小相同
		sqlCahce.setMapPosition(size);

		return sqlCahce;

	}

	/**
	 * 设置为指定的位置
	 * 
	 * @return 2016年12月23日
	 */
	private void setPutPos(SqlCacheInfoBean cacheResult, int pos) {
		cacheResult.setPutOption(pos);
	}

	/**
	 * 获取读取的指针信息
	 * 
	 * @return 2016年12月23日
	 */
	private long getPutPos(SqlCacheInfoBean cacheResult) {
		return cacheResult.getPutOption();
	}

	public void putCacheData(ProxyBuffer buffer, SqlCacheMapFileBean cacheResult)
			throws IOException, InterruptedException {

		// 进行加锁操作
		boolean lock = false;

		try {
			cacheResult.getSemap().acquire();

			lock = true;

			long writeLength = cacheResult.getPutOption() + buffer.writeIndex - buffer.readMark;

			// 重新标识出通道的大小
			cacheResult.getChannel().position(writeLength);

			// 仅在大于内存映射区域后进行文件大小的设置
			if (writeLength > cacheResult.getMapPosition()) {
				// 写入磁盘
				cacheResult.getMappedBuffer().force();

				// 进行文件的扩容
				cacheResult.getRandomFile().setLength(writeLength);

				// 重新加载通道
				cacheResult.getMappedBuffer().load();

				// 初始大小与内存大小相同
				cacheResult.setMapPosition(writeLength);

				// cacheResult.getChannel().map(mode, position, size)
			}

			// 设置起始位置
			buffer.getBuffer().position(buffer.readMark);

			buffer.getBuffer().limit(buffer.writeIndex);

			cacheResult.getMappedBuffer().limit((int) getPutPos(cacheResult) + buffer.writeIndex);

			cacheResult.getMappedBuffer().put(buffer.getBuffer());
			// 进行指针的位移操作
			setPutPos(cacheResult, (int) getPutPos(cacheResult) + buffer.writeIndex);

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

		long length = cacheResult.getPutOption();

		// 如果当前领移的数据超过了大小，则不处理
		if (offset > length) {
			return -1;
		}

		ByteBuffer buffer = proBuffer.getBuffer();

		MappedByteBuffer mapbuffer = cacheResult.getMappedBuffer();

		// 检查结果集是否需要多次的返回
		if (buffer.limit() > length) {

			mapbuffer.position(0);
			for (int i = 0; i < length; i++) {
				proBuffer.writeByte(mapbuffer.get());
				// proBuffer.writeByte(unsafe.getByte(getIndex(cacheResult,
				// i)));
			}
		} else {
			int startPosition = (int) offset;
			long endPosition = offset;
			// 如果最后一次数据，则以最长的标识为准备，不能超过
			if (offset + buffer.limit() >= length) {
				endPosition = length;
			}
			// 中间的数据按依稀进行读取
			else {
				endPosition = offset + buffer.limit();
			}

			mapbuffer.position(startPosition);
			// 进行数据填充
			for (int i = startPosition; i < endPosition; i++) {
				proBuffer.writeByte(mapbuffer.get());
			}

		}

		return length;

	}

	public void close(SqlCacheMapFileBean cacheInfo) {

		if (null != cacheInfo) {

			// 写入磁盘
			cacheInfo.getMappedBuffer().force();

			// 关闭流将文件刷入磁盘中
			IOUtils.colse(cacheInfo.getChannel());
			IOUtils.colse(cacheInfo.getRandomFile());

			// 缓存结果完成后，需要删除文件
			new File(cacheInfo.getFileName()).delete();

		}
	}

	/**
	 * 进行清理数据
	 * 
	 * @param cacheResult
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public SqlCacheMapFileBean clean(SqlCacheMapFileBean cacheResult) throws IOException, InterruptedException {

		SqlCacheMapFileBean result = null;

		if (null != cacheResult) {
			// 进行加锁操作
			boolean lock = false;

			try {
				cacheResult.getSemap().acquire();
				lock = true;

				close(cacheResult);
				// 重新创建文件缓存
				result = this.createCacheFile(null, cacheResult.getCacheSize());

			} catch (IOException e) {
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
