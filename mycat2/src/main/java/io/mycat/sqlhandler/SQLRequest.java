/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlhandler;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLStatement;
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
    public <T> T beforeCommentAsJson(Class<T> c){
        List<SQLCommentHint> headHintsDirect = ast.getHeadHintsDirect();
        List<String> beforeCommentsDirect = ast.getBeforeCommentsDirect();
        List<String> afterCommentsDirect = ast.getAfterCommentsDirect();
      return   Optional.ofNullable(headHintsDirect).filter(i->!i.isEmpty())
              .map(i->i.get(0)).map(i->i.getText().trim())
                .map(i-> {
                    return c.cast(JsonUtil.from(SqlHints.unWrapperHint(i),c));
                }).orElse(null);
    }
}
