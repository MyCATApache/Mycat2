package io.mycat.mycat2.bufferTest;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.mycat.mycat2.beans.SqlCacheMapFileBean;
import io.mycat.mycat2.cmds.sqlCmds.mapcache.MapFileCacheImp;
import io.mycat.proxy.ProxyBuffer;

public class TestMapFile {

	public static void main(String[] args) {
		MapFileCacheImp mapFile = new MapFileCacheImp();

		ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(512));

		SqlCacheMapFileBean cacheBean = null;
		try {

			for (int i = 1; i < 33; i++) {
				buffer.writeByte((byte) i);
			}
			cacheBean = mapFile.getAndPutCacheObject(buffer, 32);
			
			System.out.println("文件目录:"+cacheBean.getFileName());

			addData(mapFile, buffer, cacheBean, 33, 65);
			addData(mapFile, buffer, cacheBean, 65, 97);
			addData(mapFile, buffer, cacheBean, 97, 129);

			ProxyBuffer bufferGet = new ProxyBuffer(ByteBuffer.allocateDirect(32));

			for (int i = 0; i < 5; i++) {
				bufferGet.reset();
				getData(mapFile, bufferGet, cacheBean, i * 32);
				System.out.println();
				System.out.println();
				System.out.println();
			}

			mapFile.close(cacheBean);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void addData(MapFileCacheImp mapFile, ProxyBuffer buffer, SqlCacheMapFileBean cacheBean, int srart,
			int end) throws IOException, InterruptedException {
		buffer.reset();
		for (int i = srart; i < end; i++) {
			buffer.writeByte((byte) i);
		}
		mapFile.putCacheData(buffer, cacheBean);
	}

	private static void getData(MapFileCacheImp mapFile, ProxyBuffer bufferGet, SqlCacheMapFileBean cacheBean,
			int offset) throws IOException {

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
