package io.mycat.util;

import java.io.Closeable;

/**
 * 流的公共方法
 * 
 * @since 2017年9月4日 下午6:41:11
 * @version 0.0.1
 * @author liujun
 */
public class IOUtils {

	/**
	 * 通用的关闭方法
	 * 
	 * @param colose
	 */
	public static void colse(Closeable colose) {
		if (null != colose) {
			try {
				colose.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
