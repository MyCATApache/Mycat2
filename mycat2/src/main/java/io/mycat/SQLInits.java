package io.mycat;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import lombok.SneakyThrows;
import org.apache.curator.shaded.com.google.common.io.Files;

import java.io.BufferedWriter;
import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class SQLInits {

    @SneakyThrows
    public static void main(String[] args) {
        URL resource = SQLInits.class.getResource("/mycat2init.sql");
        File file = new File(resource.toURI());
        String s = new String(Files.toByteArray(file));
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(s, DbType.mysql, false);
        List<SQLStatement> collect = sqlStatements.stream().filter(i -> !(i instanceof MySqlHintStatement)).collect(Collectors.toList());
        try(BufferedWriter bufferedWriter = Files.newWriter(file, StandardCharsets.UTF_8);){
            for (SQLStatement sqlStatement : collect) {
                bufferedWriter.append(sqlStatement.toString());
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        }



        System.out.println();
    }
}
