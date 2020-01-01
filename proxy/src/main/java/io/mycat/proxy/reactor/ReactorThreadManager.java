package io.mycat.proxy.reactor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ReactorThreadManager {
    final ArrayList<MycatReactorThread> list;


    public ReactorThreadManager(List<MycatReactorThread> list) {
        this.list = new ArrayList<>(list);
    }

    public synchronized MycatReactorThread getRandomReactor() {
        return list.get(ThreadLocalRandom.current().nextInt(0, list.size() ));
    }

    public synchronized void add(MycatReactorThread thread) {
        list.add(thread);
    }
    public synchronized void remove(MycatReactorThread thread) {
        list.remove(thread);
    }

}