package io.mycat.router;

import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiver;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * @author jamie12221
 *  date 2019-05-05 23:56
 **/
public abstract class MycatRouterTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  protected MycatRouter loadModule(String module) {
    try {
      String rootPath = Paths.get(this.getClass().getClassLoader().getResource(module).toURI())
                            .toAbsolutePath().toString();
      ConfigReceiver cr = ConfigLoader.load(rootPath, 0);
      MycatRouterConfig config = new MycatRouterConfig(cr, null);
      return new MycatRouter(config);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}
