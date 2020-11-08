package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLCommentHint;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.*;

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
    public Optional<Map<String,Object>> getAnyJson(){
        ArrayList<String> res = new ArrayList<>();
        List<SQLCommentHint> headHintsDirect = ast.getHeadHintsDirect();
        List<String> beforeCommentsDirect = ast.getBeforeCommentsDirect();
        List<String> afterCommentsDirect = ast.getAfterCommentsDirect();
        return Optional.empty();
    }
    public <T> T afterCommentAsJson(Class<T> c){
        List<String> afterCommentsDirect = ast.getAfterCommentsDirect();
      return   Optional.ofNullable(afterCommentsDirect).filter(i->!i.isEmpty()).map(i->i.get(0).trim())
                .map(i-> {
                    return c.cast(JsonUtil.from(SqlHints.unWrapperHint(i),c));
                }).orElse(null);
    }
}
