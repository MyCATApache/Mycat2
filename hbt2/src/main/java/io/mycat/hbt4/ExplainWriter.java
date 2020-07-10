package io.mycat.hbt4;

import lombok.Data;

@Data
public class ExplainWriter {
    private int column = 0;
    private StringBuilder text = new StringBuilder();
    private StringBuilder stringBuilder = new StringBuilder();
    public ExplainWriter item(String key, Object value,boolean condition){
        if (condition){
            return item(key, value);
        }
        return this;
    }
    public ExplainWriter item(String key, Object value) {
        stringBuilder.append('\"').append(key).append('\"').append(" = ");
        if (value instanceof String) {
            stringBuilder.append('\"').append(value).append('\"');
        } else {
            stringBuilder.append(value);
        }
        stringBuilder.append(", ");
        return this;
    }

    public ExplainWriter name(String name) {
        for (int i = 0; i < column; i++) {
            stringBuilder.append(" ");
        }
        stringBuilder.append(name).append("(");
        column++;
        return this;
    }

    public ExplainWriter ret() {
        column--;
        return this;
    }

    public ExplainWriter into() {
        String s = stringBuilder.toString().trim();
        stringBuilder.setLength(0);
        if (s.endsWith(",")){
            s = s.substring(0,s.length()-1);
        }
        s = s+")";
        text.append(s).append("\n");
        return this;
    }
}