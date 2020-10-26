package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@AllArgsConstructor
@Builder
public class SQLRequest<Statement extends SQLStatement> {
    private final Statement ast;

    public String getSqlString() {
        return ast.toString();
    }
   public List<String> getBeforeHints(){
     return ast.getBeforeCommentsDirect();
   }
    public List<String> getAfterHints(){
        return ast.getAfterCommentsDirect();
    }
    public Optional<Map<String,Object>> getAfterJson(){
        List<String> afterCommentsDirect = ast.getAfterCommentsDirect();
      return   Optional.ofNullable(afterCommentsDirect).filter(i->!i.isEmpty()).map(i->i.get(0).trim())
                .map(i-> JsonUtil.from(i.substring(2,i.length()-2),Map.class));
    }
}
