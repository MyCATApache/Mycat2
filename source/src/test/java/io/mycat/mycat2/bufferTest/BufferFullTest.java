package io.mycat.mycat2.bufferTest;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BufferFullTest {

	/**
	 * loaddata传送结束标识长度
	 */
	private static final int FLAGLENGTH = 4;

	/**
	 * 测试方法，验证组合读取
	 * 
	 * @param buffer
	 * @param overFlag
	 * @return
	 */
	public static byte[] TestReadByte(ByteBuffer buffer, byte[] overFlag) {
		// byte[] overFlag = new byte[FLAGLENGTH];

		if (buffer.position() >= FLAGLENGTH) {
			int opts = buffer.position();
			buffer.position(opts - FLAGLENGTH);
			buffer.get(overFlag, 0, FLAGLENGTH);
			buffer.position(opts);
		} else {

			int opts = buffer.position();
			// 计算需要移动的位数
			int moveSize = FLAGLENGTH - opts;
			int index = 0;
			// 进行数组的移动,以让出空间进行放入新的数据
			for (int i = FLAGLENGTH - moveSize; i < FLAGLENGTH; i++) {
				overFlag[index] = overFlag[i];
				index++;
			}
			// 读取数据
			buffer.position(0);
			buffer.get(overFlag, moveSize, opts);
			buffer.position(opts);
		}

		return overFlag;
	}
	
	public static void main(String[] args) {
		BufferFullTest fullTest = new BufferFullTest();
		fullTest.testPull1();
		fullTest.testPull2();
		fullTest.testPull3();
		
	}

	public void testPull1() {
		ByteBuffer buffer = ByteBuffer.allocateDirect(30);
		byte[] overFlag = new byte[FLAGLENGTH];

		buffer.put((byte) 1);
		buffer.put((byte) 2);
		buffer.put((byte) 3);
		buffer.put((byte) 4);

		overFlag = TestReadByte(buffer, overFlag);

		ByteBuffer buffer2 = ByteBuffer.allocateDirect(1);
		buffer2.put((byte) 5);

		overFlag = TestReadByte(buffer2, overFlag);

		System.out.println(Arrays.toString(overFlag));
	}
	public void testPull2() {
		ByteBuffer buffer = ByteBuffer.allocateDirect(30);
		byte[] overFlag = new byte[FLAGLENGTH];
		
		buffer.put((byte) 1);
		buffer.put((byte) 2);
		buffer.put((byte) 3);
		buffer.put((byte) 4);
		
		overFlag = TestReadByte(buffer, overFlag);
		
		ByteBuffer buffer2 = ByteBuffer.allocateDirect(2);
		buffer2.put((byte) 5);
		buffer2.put((byte) 6);
		
		overFlag = TestReadByte(buffer2, overFlag);
		
		System.out.println(Arrays.toString(overFlag));
	}
	public void testPull3() {
		ByteBuffer buffer = ByteBuffer.allocateDirect(30);
		byte[] overFlag = new byte[FLAGLENGTH];
		
		buffer.put((byte) 1);
		buffer.put((byte) 2);
		buffer.put((byte) 3);
		buffer.put((byte) 4);
		
		overFlag = TestReadByte(buffer, overFlag);
		
		ByteBuffer buffer2 = ByteBuffer.allocateDirect(3);
		buffer2.put((byte) 5);
		buffer2.put((byte) 6);
		buffer2.put((byte) 7);
		
		overFlag = TestReadByte(buffer2, overFlag);
		
		System.out.println(Arrays.toString(overFlag));
	}

}
