package io.mycat.rpc.cs;  //This is our server task.
//It uses the multithreaded server model to deal requests out to a pool
//of workers and route replies back to clients. One worker can handle
//one request at a time but one client can talk to multiple workers at
//once.

import java.util.List;
import java.util.Objects;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ServerTask implements Runnable {
  String name;
  ZContext ctx;
  private List<String> serverAddrList;
  private List<String> backendAddrList;

  public ServerTask(ZContext ctx, List<String> serverAddrList,List<String> backendAddrList) {
    Objects.requireNonNull(ctx);
    Objects.requireNonNull(serverAddrList);
    Objects.requireNonNull(backendAddrList);
    this.name = name;
    this.ctx = ctx;
    this.serverAddrList = serverAddrList;
    this.backendAddrList = backendAddrList;
  }

  @Override
  public void run() {
      //  Frontend socket talks to clients over inproc
      Socket frontend = ctx.createSocket(SocketType.ROUTER);
      serverAddrList.forEach(frontend::bind);
      //  Backend socket talks to workers over inproc
      Socket backend = ctx.createSocket(SocketType.DEALER);
      backendAddrList.forEach(backend::bind);
      //  Connect backend to frontend via a proxy
      ZMQ.proxy(frontend, backend, null);
  }
}
