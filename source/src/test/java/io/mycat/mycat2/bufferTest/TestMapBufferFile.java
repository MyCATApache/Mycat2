package io.mycat.mycat2.bufferTest;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.mycat.mycat2.beans.SqlCacheMapFileBean;
import io.mycat.mycat2.cmds.cache.mapcache.CacheInf;
import io.mycat.mycat2.cmds.cache.mapcache.MapBufferFileCacheImp;
import io.mycat.mycat2.cmds.cache.mapcache.MapFileCacheImp;
import io.mycat.proxy.ProxyBuffer;

public class TestMapBufferFile {

	public static void main(String[] args) throws Exception {
		MapBufferFileCacheImp mapFile = new MapBufferFileCacheImp();

		ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(10240));

		SqlCacheMapFileBean cacheBean = null;
		try {

			for (int i = 1; i < 33; i++) {
				buffer.writeByte((byte) i);
			}

			byte[] bufferValue = new byte[20];

			buffer.getBuffer().get(bufferValue, 0, 20);

			cacheBean = mapFile.createCacheFile(bufferValue, 10240);

			System.out.println("文件目录:" + cacheBean.getFileName());

			addData(mapFile, buffer, cacheBean, 0, 10239);
			addData(mapFile, buffer, cacheBean, 0, 10239);
			addData(mapFile, buffer, cacheBean, 0, 10239);

			ProxyBuffer bufferGet = new ProxyBuffer(ByteBuffer.allocateDirect(32));

			for (int i = 0; i < 1; i++) {
				bufferGet.reset();
				getData(mapFile, bufferGet, cacheBean, 0);
				System.out.println();
				System.out.println();
				System.out.println();
			}

			// mapFile.close(cacheBean);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void addData(CacheInf<SqlCacheMapFileBean> mapFile, ProxyBuffer buffer,
			SqlCacheMapFileBean cacheBean, int srart, int end) throws Exception {
		buffer.reset();
		for (int i = srart; i < end; i++) {
			buffer.writeByte((byte) i);
		}
		buffer.readIndex = buffer.writeIndex;

		mapFile.putCacheData(buffer, cacheBean);
	}

	private static void getData(CacheInf<SqlCacheMapFileBean> mapFile, ProxyBuffer bufferGet,
			SqlCacheMapFileBean cacheBean, int offset) throws IOException, InterruptedException {

		mapFile.getByte(bufferGet, cacheBean, offset);

		ByteBuffer bufferss = bufferGet.getBuffer();

		for (int i = 0; i < bufferss.position(); i++) {
			if (i % 10 == 0) {
				System.out.println(bufferss.get(i));
			} else {
				System.out.print(bufferss.get(i) + "\t");
			}
		}
	}

}
