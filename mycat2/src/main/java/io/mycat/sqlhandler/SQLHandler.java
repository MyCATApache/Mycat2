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

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;

/**
 * 数据库代理的执行器, 调用后端数据库接口.
 * 承接前端数据,代理数据,后端数据.
 *
 * @param <Statement> 已经分析优化或重写后的SQL语法树
 * @author wangzihaogithub 2020年4月18日23:09:18
 */
public interface SQLHandler<Statement extends SQLStatement> {
    default Future<Void> execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response){
        return response.sendError(new MycatException(request.getAst()+" not Implemented"));
    }
    public Class getStatementClass();
}
