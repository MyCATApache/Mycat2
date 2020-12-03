package io.mycat.configRaft;

import static io.mycat.configRaft.N0.run;

public class N2 {
    public static void main(String[] args) throws Exception {
        //d:/tmp/server1 election_test 127.0.0.1:8081::100 127.0.0.1:8081::100,127.0.0.1:8082::40,127.0.0.1:8083::40
        RaftLog simpleClient = new RaftLog("d:/tmp/n3",
                "election_test","127.0.0.1:8083::40",
                "127.0.0.1:8081::100,127.0.0.1:8082::40,127.0.0.1:8083::40"
                );
        run(simpleClient);
    }
}
