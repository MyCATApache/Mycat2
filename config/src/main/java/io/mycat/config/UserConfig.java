package io.mycat.config;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class UserConfig {
    private String username;
    private String password;
    private String ip;
}