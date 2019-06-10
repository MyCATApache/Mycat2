//package io.mycat.rpc;
//
//import java.util.Arrays;
//import org.zeromq.SocketType;
//import org.zeromq.ZContext;
//import org.zeromq.ZMQ;
//import org.zeromq.ZMQ.Poller;
//import org.zeromq.ZMQ.Socket;
//import org.zeromq.ZMsg;
//
//public class Server {
//
//  private static final String INTERNAL_SERVICE_PREFIX = "mmi.";
//  private static final int HEARTBEAT_LIVENESS = 3;                                      // 3-5 is reasonable
//  private static final int HEARTBEAT_INTERVAL = 2500;                                   // msecs
//  private static final int HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
//  Poller poller;
//  public Server(ZContext context ,String... address) {
//    Socket server = context.createSocket(SocketType.REP);
//    Arrays.stream(address).forEach(server::bind);
//   = context.createPoller(0);
//  }
//
//  public void process(){
//
//  }
//
//  public static void main(String[] args) {
//    ZContext context = new ZContext();
//
//    String address = "inproc://inproc";
//
//    new Thread(() -> {
//      asyncClient.connect(address);
//      while (true) {
//        send(asyncClient);
//        ZMQ.sleep(1);
//      }
//    }).start();
////    new Thread(()->{
////    reveiver(context, asyncClient, address);
////  }).process();
//  }
//
//  private static void reveiver(ZContext context, Socket asyncClient, String... address) {
//    Socket socket = context.createSocket(SocketType.REP);
//    for (String add : address) {
//      socket.bind(add);
//    }
//
//    while (true) {
//      byte[] recv = socket.recv();
//      boolean send = socket.send("1111");
//      ZMsg zFrames = ZMsg.recvMsg(asyncClient);
//      System.out.println(new String(recv));
//
//    }
//
//  }
//
//  private static void send(Socket asyncClient) {
//    ZMsg msg = new ZMsg();
//    msg.addFirst("111");
//    msg.addFirst(new byte[]{});
//    boolean send = msg.send(asyncClient);
//    int errno = asyncClient.errno();
//    msg.destroy();
//  }
//}