package io.mycat.test;

import io.mycat.MycatCore;
import io.mycat.ProxyBeanProviders;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Assert;

/**
 * @author jamie12221 date 2019-05-23 16:53
 **/
public abstract class ModualTest {

  final static String url = "jdbc:mysql://localhost:8066/test";
  final static String username = "root";
  final static String password = "123456";

  static {
    // 加载可能的驱动
    List<String> drivers = Arrays.asList(
        "com.mysql.jdbc.Driver");

    for (String driver : drivers) {
      try {
        Class.forName(driver);
      } catch (ClassNotFoundException ignored) {
      }
    }
  }

  public static void loadModule(String module, ProxyBeanProviders proxyBeanProviders,
      TestCallback task)
      throws InterruptedException, ExecutionException, IOException {
    loadModule(module, proxyBeanProviders, new MycatMonitorLogCallback(), task);
  }

  public static void loadModule(String module, ProxyBeanProviders proxyBeanProviders,
      MycatMonitorCallback callback,
      TestCallback task)
      throws IOException, ExecutionException, InterruptedException {
    String resourcesPath = ProxyRuntime.getResourcesPath(ModualTest.class);
    Path resolve = Paths.get(resourcesPath).resolve("io/mycat/test/jdbc").resolve(module);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final CompletableFuture<String> future = new CompletableFuture<>();
    MycatCore.startup(resolve.toAbsolutePath().toString(), proxyBeanProviders, callback,
        new AsyncTaskCallBack() {
          @Override
          public void onFinished(Object sender, Object result, Object attr) {
            executor.submit(() -> {
              try (Connection connection = getConnection()) {
                task.test(future, connection);
              } catch (Exception e) {
                Assert.fail(e.toString());
              }finally {
                ProxyRuntime.INSTANCE.exit();
              }
            });
          }

          @Override
          public void onException(Exception e, Object sender, Object attr) {
            Assert.fail(e.toString());
            ProxyRuntime.INSTANCE.exit();
          }

        });
    future.get();
  }

  public static void compelete(Object fulture) {
    CompletableFuture completableFuture = (CompletableFuture) fulture;
    completableFuture.complete(null);
  }

  public static Connection getConnection() throws SQLException {
    Connection connection = null;
    connection = DriverManager
                     .getConnection(getUrl(), getUsername(),
                         getPassword());
    return connection;
  }

  public static String getUrl() {
    return url;
  }

  public static String getUsername() {
    return username;
  }

  public static String getPassword() {
    return password;
  }
}
