package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
@Data
public class ServerInfo {
    public String server = "127.0.0.1:6066";
    public List<String> serverList = new ArrayList<>();
}
