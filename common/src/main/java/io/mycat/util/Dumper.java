package io.mycat.util;

import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ToString
public class Dumper {
    final List<String> textList = new ArrayList<>();

    public static Dumper create() {
        return new Dumper();
    }

    public static Dumper create(Map<String, Object> map) {
        if (map == null) {
            return create();
        }
        Dumper textItem = create();
        map.entrySet().stream().map(i -> i.toString()).forEach(c -> textItem.addText(c));
        return textItem;
    }

    public static Dumper create(List<String> list) {
        if (list == null) {
            return create();
        }
        Dumper textItem = create();
        list.forEach(c -> textItem.addText(c));
        return textItem;
    }

    public Dumper addText(String text) {
        textList.add(text);
        return this;
    }

    public Dumper addText(String key, Object value) {
        textList.add(key + ":" + value);
        return this;
    }

    public String toString(CharSequence delimiter) {
        return textList.stream().distinct().collect(Collectors.joining(delimiter));
    }

    public List<String> toStringList() {
        return new ArrayList<>(textList);
    }
}