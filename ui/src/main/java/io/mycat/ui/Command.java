package io.mycat.ui;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Optional;

@Getter
@ToString
@EqualsAndHashCode
public class Command {
    SchemaObjectType type;
    String schema;
    String table;
    String datasource;
    String cluster;

    public Command(SchemaObjectType type, String schema, String table) {
        this.type = type;
        this.schema = schema;
        this.table = table;
    }

    public Command(SchemaObjectType type, String value) {
        this.type = type;
        switch (type) {
            case SCHEMA:
            case SHARDING_TABLES:
            case GLOBAL_TABLES:
            case SINGLE_TABLES:
                this.schema = value;
                break;
            case CLUSTER:
                this.cluster = value;
                break;
            case DATASOURCE:
                this.datasource = value;
                break;
        }

    }

    public static Optional<Command> parsePath(String text) {
        String[] split = text.split("/");
        if (split.length == 4) {
            switch (split[split.length - 2].toLowerCase()) {
                case "schemas": {
                    return Optional.of(new Command(SchemaObjectType.SCHEMA, split[split.length - 1]));
                }
                case "clusters": {
                    return Optional.of(new Command(SchemaObjectType.CLUSTER, split[split.length - 1]));
                }
                case "datasources": {
                    return Optional.of(new Command(SchemaObjectType.DATASOURCE, split[split.length - 1]));
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + split[split.length - 2]);
            }

        }
        if (split.length == 6) {
            switch (split[split.length - 2].toLowerCase()) {
                case "shardingtables": {
                    return Optional.of(new Command(SchemaObjectType.SHARDING_TABLE, split[split.length - 3], split[split.length - 1]));
                }
                case "globaltables": {
                    return Optional.of(new Command(SchemaObjectType.GLOBAL_TABLE, split[split.length - 3], split[split.length - 1]));
                }
                case "singletables": {
                    return Optional.of( new Command(SchemaObjectType.SINGLE_TABLE, split[split.length - 3], split[split.length - 1]));
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + split[split.length - 2]);
            }

        }
        if (split.length == 5) {
            switch (split[split.length - 1].toLowerCase()) {
                case "shardingtables": {
                    return  Optional.of(new Command(SchemaObjectType.SHARDING_TABLES, split[split.length - 2]));
                }
                case "globaltables": {
                    return Optional.of( new Command(SchemaObjectType.GLOBAL_TABLES, split[split.length - 2]));
                }
                case "singletables": {
                    return Optional.of( new Command(SchemaObjectType.SINGLE_TABLES, split[split.length - 2]));
                }

                default:
                    throw new IllegalStateException("Unexpected value: " + split[split.length - 1].toLowerCase());
            }

        }
        return Optional.empty();
    }
}
