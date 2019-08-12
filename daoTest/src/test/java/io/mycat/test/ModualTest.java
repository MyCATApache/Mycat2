package io.mycat.test;

import io.mycat.MycatCore;
import io.mycat.MycatException;
import io.mycat.ProxyBeanProviders;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiver;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.test.jdbc.TestGettingConnetionCallback;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Assert;

/**
 * @author jamie12221 date 2019-05-23 16:53
 **/
public abstract class ModualTest {

  //  static {
//    new Thread(()->{
//      try {
//        DbStartUp.start();
//      }finally {
//        DbStartUp.stop();
//      }
//    }).start();
  public static void loadModule(String module, ProxyBeanProviders proxyBeanProviders,
      MycatMonitorCallback callback,
        TestGettingConnetionCallback gettingConnetionCallback)
      throws IOException, ExecutionException, InterruptedException {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final CompletableFuture<String> future = new CompletableFuture<>();
    executor.submit(() -> {
      try {
        gettingConnetionCallback.test(future);
      }catch (Exception e){
        e.printStackTrace();
      }finally {
        MycatCore.exit(new MycatException("normal"));
        future.complete(null);
      }
    });
    future.get();
  }
  public static void loadModule11(String module, ProxyBeanProviders proxyBeanProviders,
      MycatMonitorCallback callback,
      TestGettingConnetionCallback gettingConnetionCallback)
      throws Exception {
    String resourcesPath = ProxyRuntime.getResourcesPath(ModualTest.class);
    String rootResourcePath = Paths.get(resourcesPath).resolve("io/mycat/test/jdbc").resolve(module).toAbsolutePath().toString();
    ConfigReceiver cr = ConfigLoader.load(rootResourcePath);
    ProxyRuntime runtime = new ProxyRuntime(cr);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final CompletableFuture<String> future = new CompletableFuture<>();
    MycatCore.startup(runtime, callback,
        new AsyncTaskCallBack() {
          @Override
          public void onFinished(Object sender, Object result, Object attr) {
            executor.submit(() -> {
              try {
                gettingConnetionCallback.test(future);
              }catch (Exception e){
                e.printStackTrace();
              }finally {
                MycatCore.exit(new MycatException("normal"));
                future.complete(null);
              }
            });
          }

          @Override
          public void onException(Exception e, Object sender, Object attr) {
            e.printStackTrace();
            Assert.fail(e.toString());
            MycatCore.exit(e);
            future.complete(null);
          }

        });
    future.get();
  }

  public static void compelete(Object fulture) {
    CompletableFuture completableFuture = (CompletableFuture) fulture;
    completableFuture.complete(null);
  }



}
