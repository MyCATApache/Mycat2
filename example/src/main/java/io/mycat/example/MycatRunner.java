package io.mycat.example;

import io.mycat.MycatCore;
import org.junit.Before;

public class MycatRunner {
    static boolean run = false;

    public synchronized static void main(String[] args) throws Exception {
        checkRunMycat();
    }

    public synchronized static void checkRunMycat() throws Exception {
        if (!run) {
//            MycatCore mycatCore = new MycatCore();
//            mycatCore.start();
            run = true;
        }
    }
}
