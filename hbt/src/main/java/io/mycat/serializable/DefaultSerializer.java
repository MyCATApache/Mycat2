package io.mycat.serializable;

import lombok.SneakyThrows;

import java.io.IOException;

public class DefaultSerializer implements DiskArrayList.Serializer<Object[]> {
    public static final DefaultSerializer INSTANCE = new DefaultSerializer();
    @Override
    @SneakyThrows
    public Object[] read(ExtendedDataInputStream oo) throws IOException {
        return oo.readObjects();
    }

    @Override
    @SneakyThrows
    public void write(Object[] object, ExtendedDataOutputStream oo) throws IOException {
        oo.writeObjects((Object[]) object);
    }
}
