package io.mycat.mycat2.sqlannotations;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationManager;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationManagerImpl;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.SQLType;
import io.mycat.proxy.ProxyRuntime;

/**
 * Created by jamie on 2017/9/22.
 */
public class AnnotationProcessor {
    private static final AtomicReference<DynamicAnnotationManager> dynamicAnnotationManager = new AtomicReference<>();
    private static final AtomicInteger count = new AtomicInteger(0);
    private static final AnnotationProcessor ourInstance = new AnnotationProcessor();
    private static final String ACTIONS_PATH = "actions.yaml";
    private static final String ANNOTATIONS_PATH = "annotations.yaml";
    private static WatchService watcher;

    public static AnnotationProcessor getInstance() {
        return ourInstance;
    }

    private AnnotationProcessor() {
        try {
            init();
            watcher = FileSystems.getDefault().newWatchService();
            Path path = Paths.get(AnnotationProcessor.class.getClassLoader().getResource("").toURI());
            System.out.println("=>系统监视的路径是" + path);
            path.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ProxyRuntime.INSTANCE.addBusinessJob(AnnotationProcessor::listen);//todo 检查这个线程池是否妥当
    }

    /**
     * 返回false代表没有匹配的action
     * @param context
     * @param session
     * @param collect
     * @return
     */
    public boolean parse(BufferSQLContext context, MycatSession session, List collect) {
        if (context.getTableCount() != 0) {
            int sqltype = context.getSQLType();
            String schemaName = session.schema.getName();
            int[] intHashTables;
            if (sqltype < 15 && sqltype > 10) {   //TODO  这里可能有更多的类型
                int size = context.getTableCount();
                intHashTables = new int[size];
                for (int j = 0; j < size; j++) {
                    intHashTables[j] = context.getTableIntHash(j);
                }
            }else{
            	intHashTables = new int[0];
            }
            try {
                dynamicAnnotationManager.get().collect(schemaName, SQLType.getSQLTypeByValue(sqltype), intHashTables, context, collect);
            return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private static void init() {
        DynamicAnnotationManager manager = null;
        try {
            manager = new DynamicAnnotationManagerImpl(ACTIONS_PATH, ANNOTATIONS_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (manager != null) {
            dynamicAnnotationManager.set(manager);
        }
    }

    public static void listen() {
        try {
            while (true) {
                WatchKey key = watcher.take();//todo 线程复用,用 poll比较好?
                boolean flag = false;
                for (WatchEvent<?> event: key.pollEvents()) {
                    String str = event.context().toString();
                    if ("actions.yaml".equals(str)|| "annotations.yaml".equals(str)) {
                        flag=true;
                        break;
                    }
                }
                if (flag){
                    System.out.println("动态注解更新次数" + count.incrementAndGet());
                    init();
                }
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
