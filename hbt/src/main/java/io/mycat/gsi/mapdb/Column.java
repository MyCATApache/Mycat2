package io.mycat.gsi.mapdb;

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.TypeUtils;
import lombok.Getter;
import org.mapdb.Serializer;

import java.util.Objects;

@Getter
public class Column{
    private String name;
    private Serializer serializer;
    private Class type;
    public Column(String name, Serializer serializer,Class type) {
        this.name = name;
        this.serializer = Objects.requireNonNull(serializer);
        this.type = Objects.requireNonNull(type);
    }

    public Object cast(Object value){
        Object cast = TypeUtils.cast(value, type, ParserConfig.getGlobalInstance());
        return cast;
    }
    @Override
    public String toString() {
        return name;
    }
}
