package io.mycat.ui;

import javafx.scene.control.TreeItem;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ObjectItem {
    String id;
    Object object;
    String text;

    public static ObjectItem ofSchema(String schema) {
        TreeItem root = new TreeItem("root");
        TreeItem schemas = new TreeItem("schemas");
        root.getChildren().add(schemas);
        TreeItem bottomSchema = new TreeItem(schema);
        schemas.getChildren().add(bottomSchema);
        String path = UIMain.getTextPath(bottomSchema);
        return ObjectItem.builder().id(path).text(schema).object(schema).build();
    }

    public static ObjectItem ofShardingTables(String schema) {
        TreeItem root = new TreeItem("root");
        TreeItem schemas = new TreeItem("schemas");
        root.getChildren().add(schemas);
        TreeItem bottomSchema = new TreeItem(schema);
        schemas.getChildren().add(bottomSchema);

        TreeItem bottomNode = new TreeItem("shardingtables");
        bottomSchema.getChildren().add(bottomNode);
        String path = UIMain.getTextPath(bottomNode);
        return ObjectItem.builder().id(path).text("shardingtables").object(schema).build();
    }

    public static ObjectItem ofShardingTable(String schema, String table) {
        TreeItem root = new TreeItem("root");
        TreeItem schemas = new TreeItem("schemas");
        root.getChildren().add(schemas);
        TreeItem bottomSchema = new TreeItem(schema);
        schemas.getChildren().add(bottomSchema);

        TreeItem bottomNode = new TreeItem("shardingtables");
        bottomSchema.getChildren().add(bottomNode);


        String path = UIMain.getTextPath(bottomNode);
        return ObjectItem.builder().id(path).text(schema).object(table).build();
    }

    public static ObjectItem ofGlobalTables(String schema) {
        TreeItem root = new TreeItem("root");
        TreeItem schemas = new TreeItem("schemas");
        root.getChildren().add(schemas);
        TreeItem bottomSchema = new TreeItem(schema);
        schemas.getChildren().add(bottomSchema);

        TreeItem bottomNode = new TreeItem("globaltables");
        bottomSchema.getChildren().add(bottomNode);
        String path = UIMain.getTextPath(bottomNode);
        return ObjectItem.builder().id(path).text("globaltables").object(schema).build();
    }

    public static ObjectItem ofGlobalTable(String schema, String table) {
        TreeItem root = new TreeItem("root");
        TreeItem schemas = new TreeItem("schemas");
        root.getChildren().add(schemas);
        TreeItem bottomSchema = new TreeItem(schema);
        schemas.getChildren().add(bottomSchema);

        TreeItem bottomNode = new TreeItem("globaltables");
        bottomSchema.getChildren().add(bottomNode);
        String path = UIMain.getTextPath(bottomNode);
        return ObjectItem.builder().id(path).text(schema).object(table).build();
    }

    public static ObjectItem ofSingleTables(String schema) {
        TreeItem root = new TreeItem("root");
        TreeItem schemas = new TreeItem("schemas");
        root.getChildren().add(schemas);
        TreeItem bottomSchema = new TreeItem(schema);
        schemas.getChildren().add(bottomSchema);

        TreeItem bottomNode = new TreeItem("singletables");
        bottomSchema.getChildren().add(bottomNode);
        String path = UIMain.getTextPath(bottomNode);
        return ObjectItem.builder().id(path).text("singletables").build();
    }

    public static ObjectItem ofSingleTable(String schema, String table) {
        TreeItem root = new TreeItem("root");
        TreeItem schemas = new TreeItem("schemas");
        root.getChildren().add(schemas);
        TreeItem bottomSchema = new TreeItem(schema);
        schemas.getChildren().add(bottomSchema);

        TreeItem bottomNode = new TreeItem("singletables");
        bottomSchema.getChildren().add(bottomNode);

        TreeItem tableItem = new TreeItem(table);

        bottomNode.getChildren().add(tableItem);

        String path = UIMain.getTextPath(tableItem);

        return ObjectItem.builder().id(path).text(table).object(table).build();
    }

    public static ObjectItem ofDatasource(String datasource) {
        TreeItem root = new TreeItem("root");
        TreeItem datasources = new TreeItem("datasources");
        root.getChildren().add(datasources);

        TreeItem datasourceItem = new TreeItem("datasource");
        datasources.getChildren().add(datasourceItem);

        String path = UIMain.getTextPath(datasourceItem);
        return ObjectItem.builder().id(path).text(datasource).object(datasource).build();
    }

    public static ObjectItem ofCluster(String cluster) {
        TreeItem root = new TreeItem("root");
        TreeItem datasources = new TreeItem("clusters");
        root.getChildren().add(datasources);

        TreeItem clusterItem = new TreeItem("cluster");
        datasources.getChildren().add(clusterItem);

        String path = UIMain.getTextPath(clusterItem);
        return ObjectItem.builder().id(path).text(cluster).object(cluster).build();
    }
}
