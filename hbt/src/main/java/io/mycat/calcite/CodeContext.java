package io.mycat.calcite;

import io.mycat.util.JsonUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class CodeContext implements Serializable {
    String name;
    String code;

    public CodeContext(String name, String code) {
        this.name = name;
        this.code = code;
    }

    public String toJson() {
        Map<String,String> map = new HashMap<>();
        map.put("name",name);
        map.put("code",code);
        return JsonUtil.toJson(map);
    }

    public  static CodeContext fromJson(String text){
        Map<String,String> map = JsonUtil.from(text, Map.class);
        return new CodeContext(map.get("name"),map.get("code"));
    }
}
