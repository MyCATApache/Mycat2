//package io.mycat.manager.commands;
//
//import com.alibaba.fastsql.sql.SQLUtils;
//import com.alibaba.fastsql.sql.ast.SQLExpr;
//import com.alibaba.fastsql.sql.ast.SQLName;
//import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
//import com.alibaba.fastsql.sql.ast.expr.SQLLiteralExpr;
//import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
//import com.alibaba.fastsql.sql.ast.expr.SQLTextLiteralExpr;
//import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement;
//import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
//import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
//import io.mycat.MycatDataContext;
//import io.mycat.TableHandler;
//import io.mycat.client.MycatRequest;
//import io.mycat.metadata.MetadataManager;
//import io.mycat.util.Response;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVRecord;
//
//import java.io.InputStreamReader;
//import java.net.URL;
//import java.net.URLConnection;
//import java.nio.charset.Charset;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Objects;
//import java.util.Optional;
//import java.util.stream.Collectors;
//
//public class LoaddataCommand implements ManageCommand {
//    @Override
//    public String statement() {
//        return "loaddata";
//    }
//
//    @Override
//    public String description() {
//        return "loaddata...";
//    }
//
//    @Override
//    public void handle(MycatRequest request, MycatDataContext context, Response response) {
//
//    }
//
//    @Override
//    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
//        try {
//            if (request.getText().startsWith("loaddata")) {
//                MySqlLoadDataInFileStatement stmt = (MySqlLoadDataInFileStatement) SQLUtils.parseSingleMysqlStatement(request.getText());
//                boolean lowPriority = stmt.isLowPriority();
//                boolean concurrent = stmt.isConcurrent();
//                boolean local = stmt.isLocal();
//
//                String fileName = SQLUtils.normalize(stmt.getFileName().toString());
//
//                boolean replicate = stmt.isReplicate();
//                boolean ignore = stmt.isIgnore();
//
//                String schemaName = null;
//                String tableName = null;
//                SQLName target = stmt.getTableName();
//
//                if (target instanceof SQLPropertyExpr) {
//                    schemaName = SQLUtils.normalize(((SQLPropertyExpr) target).getOwner().toString());
//                    tableName = SQLUtils.normalize(((SQLPropertyExpr) target).getName());
//                } else {
//                    tableName = SQLUtils.normalize(target.getSimpleName());
//                }
//                if (schemaName == null) {
//                    schemaName = context.getDefaultSchema();
//                }
//                if (schemaName == null) {
//                    throw new IllegalArgumentException("please use schema!");
//                }
//                Charset charset = Charset.forName(Objects.requireNonNull(stmt.getCharset()));
//
//                char columnsTerminatedBy = Optional.ofNullable(stmt.getColumnsTerminatedBy())
//                        .map(i -> ((SQLTextLiteralExpr) i).getText().charAt(0)).orElse('\t');
//
//                boolean columnsEnclosedOptionally = stmt.isColumnsEnclosedOptionally();
//
//                Character columnsEnclosedBy = Optional.ofNullable(stmt.getColumnsEnclosedBy())
//                        .map(j -> ((SQLTextLiteralExpr) j).getText().charAt(0)).orElse(null);
//
//                Character columnsEscaped = Optional.ofNullable(stmt.getColumnsEscaped()).map(j -> {
//                    return ((SQLTextLiteralExpr) j).getText().charAt(0);
//                }).orElse(null);
//
//                SQLLiteralExpr linesStartingBy = stmt.getLinesStartingBy();
//
//                char linesTerminatedBy = Optional.ofNullable(stmt.getLinesTerminatedBy())
//                        .map(i -> ((SQLTextLiteralExpr) i).getText().charAt(0)).orElse('\n');
//
//                long ignoreLinesNumber = Optional.ofNullable(stmt.getIgnoreLinesNumber()).map(i -> Long.parseLong(i.toString())).orElse(0L);
//
//                List<SQLExpr> setList = stmt.getSetList();
//
//                List<SQLExpr> columns = stmt.getColumns();
//
//                if (columns == null || columns.isEmpty()) {
//                    //////////////////////////////////////////////////////////////
//                    TableHandler table = MetadataManager.INSTANCE.getTable(schemaName, tableName);
//                    if (table == null){
//
//                    }
//                } else {
//                    List<String> columnNames = columns.stream().map(i -> SQLUtils.normalize(i.toString())).collect(Collectors.toList());
//                }
//
//
//                URL url = new URL(fileName);
//                URLConnection urlConnection = url.openConnection();
//                InputStreamReader inputStreamReader = new InputStreamReader(urlConnection.getInputStream(), charset);
//                CSVFormat csvFormat = CSVFormat.newFormat(
//                        columnsTerminatedBy)
//                        .withRecordSeparator(linesTerminatedBy).
//                                withSkipHeaderRecord(false).withTrim(false);
//
//                if (columnsEnclosedBy != null) {
//                    csvFormat = csvFormat.withQuote(columnsEnclosedBy);
//                }
//                if (columnsEscaped != null) {
//                    csvFormat = csvFormat.withQuote(columnsEscaped);
//                }
//
//
//                Iterable<CSVRecord> records = csvFormat.parse(inputStreamReader);
//                int batch = 300;
//                int count = 0;
//
//                Iterator<CSVRecord> iterator = records.iterator();
//                while (iterator.hasNext()) {
//                    MySqlInsertStatement mySqlInsertStatement = new MySqlInsertStatement();
//                    mySqlInsertStatement.setIgnore(ignore);
//                    mySqlInsertStatement.setLowPriority(lowPriority);
//                    List<SQLInsertStatement.ValuesClause> valuesList = mySqlInsertStatement.getValuesList();
//                    while (iterator.hasNext()) {
//                        SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();
//                        valuesList.add(valuesClause);
//                        for (String s : iterator.next()) {
//                            valuesClause.addValue(SQLExprUtils.fromJavaObject(s));
//                        }
//                        count++;
//                        if (count >= batch) {
//                            send(mySqlInsertStatement);
//                        }
//                    }
//                }
//                end();
//
//
//                return false;
//            }
//        } catch (Throwable throwable) {
//
//        }
//    }
//
//    private void end() {
//
//    }
//
//    private void send(MySqlInsertStatement mySqlInsertStatement) {
//
//
//    }
//}