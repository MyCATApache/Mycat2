/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.rpc.cs;

import java.util.List;
import java.util.Objects;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

/**
 * The type Rpc proxy.
 * @author jamie12221
 */
public class RpcProxy implements Runnable {

  /**
   * The Name.
   */
  String name;
  /**
   * The Ctx.
   */
  ZContext ctx;
  private List<String> serverAddrList;
  private List<String> backendAddrList;

  /**
   * Instantiates a new Rpc proxy.
   *
   * @param ctx the ctx
   * @param serverAddrList the server addr list
   * @param backendAddrList the backend addr list
   */
  public RpcProxy(ZContext ctx, List<String> serverAddrList,List<String> backendAddrList) {
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
