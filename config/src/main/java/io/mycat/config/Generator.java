package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Data
@Builder
@NoArgsConstructor
public final class Generator {
    String clazz;
    List<String> listOptions;
    Map<String, String> kvOptions;
}

 