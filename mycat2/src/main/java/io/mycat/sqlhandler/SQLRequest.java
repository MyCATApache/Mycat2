package io.mycat.sqlhandler;

import io.mycat.client.MycatRequest;
import io.mycat.util.SQLContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Builder
public class SQLRequest<Statement> {
    private final Statement ast;
    private final SQLContext context;
    private final MycatRequest request;
}
