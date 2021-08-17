package io.mycat.ui;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableView;
import javafx.util.Callback;

public class TableViewOuter {
    TableView table;

    public TableViewOuter(TableView output) {
        this.table = output;
    }

    public void setPlaceholder(String content) {
        table.getItems().clear();
        table.getColumns().clear();
        table.setPlaceholder(new Label(content));

    }

    public TableViewOuter appendData(TableData tableData) {

        //    List<String> tableColumnList ,List<List<String>> dataValuesList
        table.getItems().clear();
        table.getColumns().clear();
        table.setPlaceholder(new Label("Loading..."));
        int ColumnCount = tableData.getColumnCount();

        for (int columnIndex = 0; columnIndex < ColumnCount; columnIndex++) {
            try {
                ResultSetMetaData resultSetMetaData = tableData.getResultSetMetaData();
                if (resultSetMetaData != null) {
                    table
                            .getColumns()
                            .add(
                                    createColumn(
                                            columnIndex,resultSetMetaData.getColumnName(columnIndex + 1)));
                }else {
                    List<String> columnNames = tableData.getColumnNames();
                    table
                            .getColumns()
                            .add(
                                    createColumn(
                                            columnIndex,columnNames.get((columnIndex ))));
                }
            } catch (SQLException e) {
                System.out.println(e);
            }
        }
        for (String[] dataValues : tableData.getDataList()) {
            ObservableList<StringProperty> data = FXCollections.observableArrayList();
            for (String value : dataValues) {
                data.add(new SimpleStringProperty(value));
            }
            table.getItems().add(data);
        }
        return this;
    }

    private TableColumn<ObservableList<StringProperty>, String> createColumn(
            final int columnIndex, String columnTitle) {
        TableColumn<ObservableList<StringProperty>, String> column = new TableColumn<>();
        String title;
        if (columnTitle == null || columnTitle.trim().length() == 0) {
            title = "Column " + (columnIndex + 1);
        } else {
            title = columnTitle;
        }
        column.setText(title);
        column.setCellValueFactory(
                new Callback<
                        CellDataFeatures<ObservableList<StringProperty>, String>, ObservableValue<String>>() {
                    @Override
                    public ObservableValue<String> call(
                            CellDataFeatures<ObservableList<StringProperty>, String> cellDataFeatures) {
                        ObservableList<StringProperty> values = cellDataFeatures.getValue();
                        if (columnIndex >= values.size()) {
                            return new SimpleStringProperty("");
                        } else {
                            return cellDataFeatures.getValue().get(columnIndex);
                        }
                    }
                });
        return column;
    }
}
