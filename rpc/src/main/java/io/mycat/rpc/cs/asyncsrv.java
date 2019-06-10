package io.mycat.rpc.cs;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

//
//Asynchronous client-to-server (DEALER to ROUTER)
//
//While this example runs in a single process, that is just to make
//it easier to start and stop the example. Each task has its own
//context and conceptually acts as a separate process.

public class asyncsrv {




  //The main thread simply starts several clients, and a server, and then
  //waits for the server to finish.

  public static void main(String[] args) throws Exception {
//        new Thread(new ClientTask()).start();
//        new Thread(new ClientTask()).start();
    ZContext context = new ZContext();
    Poller poller = context.createPoller(1);
    String serverAddr  = "inproc://localhost:5570";
    new Thread(new ServerTask(context,Collections.singletonList(serverAddr),Collections.singletonList( "inproc://localhost:backend"))).start();
    //  Launch pool of worker threads, precise number is not critical
    ZMQ.sleep(1);
    for (int threadNbr = 0; threadNbr < 5; threadNbr++) {
      new Thread(()->{
        ServerWorker serverWorker = new ServerWorker(context, "inproc://localhost:backend", false,
            100);
        serverWorker.process(100);
      }).start();
    }

    ZMQ.sleep(1);
    ClientTask clientTask = new ClientTask((short) 1,serverAddr , context, poller);
    clientTask.connect();
    ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
    service.scheduleAtFixedRate(()->{
      pending(clientTask);
    },1,1, TimeUnit.SECONDS);
    while (true){
      clientTask.process(100,false);
    }

  }

  private static void pending(ClientTask clientTask) {
    clientTask.pending(new ClientHandlerImpl());
  }
}
