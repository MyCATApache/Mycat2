package io.mycat.util.nio;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * @author jamie12221
 * @date 2019-05-20 17:57
 **/
public class NIOUtil {

  public static void close(SocketChannel channel) {
    if (channel == null) {
      return;
    }
    try {
      channel.close();
    } catch (IOException e) {
    }
  }

}
