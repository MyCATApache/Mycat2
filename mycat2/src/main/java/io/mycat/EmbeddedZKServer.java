package io.mycat;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.util.Properties;

public class EmbeddedZKServer {

    public static void startEmbeddedZKServer0(String tickTime,
                                              String dataDir,
                                              String clientPort,
                                              String initLimit,
                                              String syncLimit) throws Exception {
        Properties props = new Properties();
        props.setProperty("tickTime", tickTime);
        props.setProperty("dataDir", dataDir);
        props.setProperty("clientPort", clientPort);
        props.setProperty("initLimit", initLimit);
        props.setProperty("syncLimit", syncLimit);

        QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
        quorumConfig.parseProperties(props);
        final ZooKeeperServerMain zkServer = new ZooKeeperServerMain();
        final ServerConfig config = new ServerConfig();
        config.readFrom(quorumConfig);
        zkServer.runFromConfig(config);
    }


    public static void startDefaultZK() throws Exception {
        startEmbeddedZKServer0("2000", new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsolutePath(), "2181", "10", "5");
    }

}