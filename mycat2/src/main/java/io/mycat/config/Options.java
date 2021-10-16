package io.mycat.config;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Options {
    String createSchemaName;
    String createTableName;
    boolean persistence;
}
