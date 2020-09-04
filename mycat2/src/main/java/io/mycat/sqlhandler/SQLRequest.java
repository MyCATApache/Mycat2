package io.mycat.sqlhandler;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Builder
public class SQLRequest<Statement> {
    private final Statement ast;

    public String getSqlString() {
        return ast.toString();
    }
}
