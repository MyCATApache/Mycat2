package io.mycat;

public class ThreadUtil {

    /**
     * https://github.com/MRswz/myehr/blob/f88c00264a54ce3fb84dc093968ba4b7e174dfc3/src/com/myehr/common/util/ThreadUtil.java
     *
     * @return
     */
    public static Thread[] findAllThreads() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;

        /* 遍历线程组树，获取根线程组 */
        while (group != null) {
            topGroup = group;
            group = group.getParent();
        }
        /* 激活的线程数加倍 */
        int estimatedSize = topGroup.activeCount() * 2;
        Thread[] slackList = new Thread[estimatedSize];

        /* 获取根线程组的所有线程 */
        int actualSize = topGroup.enumerate(slackList);
        /* copy into a list that is the exact size */
        Thread[] list = new Thread[actualSize];
        System.arraycopy(slackList, 0, list, 0, actualSize);
        return (list);
    }

    public static Thread findThreadByName(String threadName) {
        Thread[] threads = findAllThreads();
        for (int i = 0; i < threads.length; i++) {
            Thread thread = threads[i];
            String name = thread.getName();
            if (name.equals(threadName)) {
                return thread;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        Thread[] threads = findAllThreads();
        for (int i = 0; i < threads.length; i++) {
            Thread thread = threads[i];
            String name = thread.getName();
            System.out.println(name);
        }
    }
}