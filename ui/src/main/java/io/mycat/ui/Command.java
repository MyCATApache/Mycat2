package io.mycat.ui;

import lombok.Getter;

@Getter
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
                this.schema = value;
                break;
            case SHARDING_TABLES:
                break;
            case GLOBAL_TABLES:
                break;
            case SINGLE_TABLES:
                break;
            case CLUSTER:
                this.cluster = value;
                break;
            case DATASOURCE:
                this.datasource = value;
                break;
        }

    }

    public static Command parsePath(String text) {
        String[] split = text.split("/");
        if (split.length == 4) {
            switch (split[split.length - 2].toLowerCase()) {
                case "schemas": {
                    return new Command(SchemaObjectType.SCHEMA, split[split.length - 1]);
                }
                case "clusters": {
                    return new Command(SchemaObjectType.CLUSTER, split[split.length - 1]);
                }
                case "datasources": {
                    return new Command(SchemaObjectType.DATASOURCE, split[split.length - 1]);
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + split[split.length - 2]);
            }

        }
        if (split.length == 6) {
            switch (split[split.length - 2].toLowerCase()) {
                case "shardingtables": {
                    return new Command(SchemaObjectType.SHARDING_TABLES, split[split.length - 3], split[split.length - 1]);
                }
                case "globaltables": {
                    return new Command(SchemaObjectType.GLOBAL_TABLES, split[split.length - 3], split[split.length - 1]);
                }
                case "singletables": {
                    return new Command(SchemaObjectType.SINGLE_TABLES, split[split.length - 3], split[split.length - 1]);
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + split[split.length - 2]);
            }

        }
        if (split.length == 5) {
            switch (split[split.length - 1].toLowerCase()) {
                case "shardingtables": {
                    return new Command(SchemaObjectType.SHARDING_TABLES, split[split.length - 2]);
                }
                case "globaltables": {
                    return new Command(SchemaObjectType.GLOBAL_TABLES, split[split.length - 2]);
                }
                case "singletables": {
                    return new Command(SchemaObjectType.SINGLE_TABLES, split[split.length - 2]);
                }

                default:
                    throw new IllegalStateException("Unexpected value: " + split[split.length - 1].toLowerCase());
            }

        }
        return null;
    }
}
