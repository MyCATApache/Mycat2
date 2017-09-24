package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationManager;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationManagerImpl;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.SQLType;
import io.mycat.proxy.ProxyRuntime;

import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    public void parse(BufferSQLContext context, MycatSession session) {
        if (context.getTableCount() != 0) {
            int sqltype = context.getSQLType();
            if (sqltype < 15 && sqltype > 10) {
                String schemaName = session.schema.getName();
                int size = context.getTableCount();
                int[] intHashTables = new int[size];
                for (int j = 0; j < size; j++) {
                    System.out.println(context.getTableName(j));
                    intHashTables[j] = context.getTableIntHash(j);
                }
                try {
                    dynamicAnnotationManager.get().process(schemaName, SQLType.getSQLTypeByValue(sqltype), intHashTables, context).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
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
                for (WatchEvent<?> event: key.pollEvents()) {
                }
                System.out.println("动态注解更新次数" + count.incrementAndGet());
                init();
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
