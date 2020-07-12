package io.mycat.hbt3;

import io.mycat.util.JsonUtil;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static com.google.common.io.Files.asCharSource;

public class Main {
    public static void main(String[] args) throws Exception {
        String defaultSchema = "db1";
        String sql = "(select count(1) from travelrecord) union all (select count(1) from travelrecord where id = 1 limit 2)";
        URL resource = Main.class.getResource("/drds.json");
        String text = asCharSource(new File(resource.toURI()), StandardCharsets.UTF_8).read();
        DrdsConfig config = JsonUtil.from(text, DrdsConfig.class);
        DrdsRunner drdsRunners = new DrdsRunner();
        drdsRunners.doAction(config, defaultSchema, sql, new ResultSetHanlderImpl());
    }

}