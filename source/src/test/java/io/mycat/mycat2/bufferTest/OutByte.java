package io.mycat.mycat2.bufferTest;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import io.mycat.proxy.ProxyBuffer;

public class OutByte {

	public static void main(String[] args) {
		System.out.println(0xff);

		ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocateDirect(1024));
		buffer.putByte(0, (byte) 0xff);
		buffer.putByte(1, (byte) 0xff);
		buffer.putByte(2, (byte) 0xff);
		buffer.putByte(3, (byte) 0x00);
		buffer.putByte(4, (byte) 0x03);

		System.out.println(buffer.getLenencInt(0));

		for (int i = 0; i < 1000; i++) {
			if (i % 20 == 0) {
				System.out.println(ThreadLocalRandom.current().nextInt(20));
			}
			else
			{
				System.out.print(ThreadLocalRandom.current().nextInt(20) + "\t");
			}
		}
	}
}
