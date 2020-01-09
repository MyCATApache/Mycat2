package io.mycat.proxy.reactor;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class ReactorThreadManager {
    final CopyOnWriteArrayList<MycatReactorThread> list;


    public CopyOnWriteArrayList<MycatReactorThread> getList() {
        return list;
    }

    public ReactorThreadManager(List<MycatReactorThread> list) {
        this.list = new CopyOnWriteArrayList<>(list);
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