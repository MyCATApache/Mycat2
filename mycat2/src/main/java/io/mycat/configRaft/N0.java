package io.mycat.configRaft;

import com.alipay.sofa.jraft.Status;

import java.util.Scanner;

public class N0 {
    public static void main(String[] args) throws Exception {
        //d:/tmp/server1 election_test 127.0.0.1:8081::100 127.0.0.1:8081::100,127.0.0.1:8082::40,127.0.0.1:8083::40
        RaftLog simpleClient = new RaftLog("d:/tmp/n",
                "election_test", "127.0.0.1:8081::100",
                "127.0.0.1:8081::100,127.0.0.1:8082::40,127.0.0.1:8083::40");
        run(simpleClient);
    }

    public static void run(RaftLog simpleClient) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()){
            String next = scanner.next();
            String[] split = next.split(":");
            if ("GET".equalsIgnoreCase(split[0])){
                simpleClient.get(split[1], new MycatTaskClosure() {
                    @Override
                    public void run(Status status) {
                        Object response = getResponse();
                        System.out.println(response);
                    }
                });
            }
            if ("SET".equalsIgnoreCase(split[0])){
                simpleClient.set(split[1], split[2], new MycatTaskClosure() {
                    @Override
                    public void run(Status status) {
                        Object response = getResponse();
                        System.out.println(response);
                    }
                });
            }
        }
    }
}
