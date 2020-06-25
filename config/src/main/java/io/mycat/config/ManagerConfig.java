package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class ManagerConfig {
    private String ip = "127.0.0.1";
    private int port = 9066;
    private List<UserConfig> users = new ArrayList<>();
}