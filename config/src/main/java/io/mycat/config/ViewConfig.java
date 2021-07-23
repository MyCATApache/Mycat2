package io.mycat.config;

import lombok.*;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
@Builder
public class ViewConfig {
    String createViewSQL;
}