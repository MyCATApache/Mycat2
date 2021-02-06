package io.vertx;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class TraceLogFileThread extends Thread {
    private final Map<Trace, OutputStream> map = new LinkedHashMap<>();

    public TraceLogFileThread() {
    }

    public TraceLogFileThread(String name) {
        super(name);
    }

    public TraceLogFileThread(Runnable target) {
        super(target);
    }

    @SneakyThrows
    @Override
    public void run() {
        try {
            init(map);
            while (!isInterrupted()) {
                writeFile(map);
                Thread.sleep(50);
            }
        } finally {
            log.info("trace stop");
            for (OutputStream stream : map.values()) {
                stream.close();
            }
        }
    }

    private static void init(Map<Trace, OutputStream> map) throws IOException {
        for (Trace value : Trace.values()) {
            String fileName = System.getProperty("user.dir") + "/.trace/" + value.name().toLowerCase() + ".txt";

            File file = new File(fileName);
            File parentFile = file.getParentFile();
            if (parentFile != null) {
                parentFile.mkdirs();
            }
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();
            map.put(value, new BufferedOutputStream(new FileOutputStream(file), 8096));
        }
    }

    public static void writeFile(Map<Trace, OutputStream> map) throws IOException {
        byte[] newLine = "\r\n".getBytes();
        for (Map.Entry<Trace, OutputStream> entry : map.entrySet()) {
            Trace trace = entry.getKey();
            OutputStream outputStream = entry.getValue();
            TraceSpan span;
            while ((span = trace.getQueue().poll()) != null) {
                byte[] bytes = span.toLogString().getBytes(StandardCharsets.UTF_8);
                outputStream.write(bytes);
                outputStream.write(newLine);
            }
            outputStream.flush();
        }
    }

}
