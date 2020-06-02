package io.mycat.mpp;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLQueryExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import lombok.Getter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.*;

public class QueryComplier {
    public void parse(String sql) {

        final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);

        TABLE_REPOSITORY.acceptDDL("CREATE TABLE `travelrecord` ( `id` bigint(20) NOT NULL AUTO_INCREMENT,`user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,`traveldate` date DEFAULT NULL,`fee` decimal(10,0) DEFAULT NULL,`days` int(11) DEFAULT NULL,`blob` longblob DEFAULT NULL);");
        TABLE_REPOSITORY.acceptDDL("CREATE TABLE `company` ( `id` bigint(20) NOT NULL AUTO_INCREMENT,`user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,`traveldate` date DEFAULT NULL,`fee` decimal(10,0) DEFAULT NULL,`days` int(11) DEFAULT NULL,`blob` longblob DEFAULT NULL);");
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) SQLUtils
                .parseSingleMysqlStatement(sql);

        TABLE_REPOSITORY.resolve(sqlSelectStatement,
                ResolveAllColumn,
                ResolveIdentifierAlias,
                CheckColumnAmbiguous);
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlSelectStatement.getSelect().getQuery();
        QueryScope scope = new QueryScope(null, query);
        query.accept(scope);
        HashMap<String, Object> objectObjectHashMap = new HashMap<>();
        visit(scope,objectObjectHashMap);
    }

    //
    void visit(Scope scope, Map<String,Object> map) {
        if (scope instanceof LeftScope){
            LeftScope scope1 = (LeftScope) scope;
            System.out.println();
        }else if(scope instanceof QueryScope){
            QueryScope scope1 = (QueryScope) scope;
            for (Scope child : scope1.getChildren()) {
                visit(child,map);
            }
        }
    }

    //
//    void c2(Scope scope) {
//        List<SQLObject> names = scope.getNames();
//        SQLTableSource from = scope.getTableSource();
//        if (from != null) {
//            if (from instanceof SQLExprTableSource) {
//                SQLExprTableSource from1 = (SQLExprTableSource) from;
//                List<SQLName> columns = from1.getColumns();
//            } else if (from instanceof SQLJoinTableSource) {
//                SQLJoinTableSource from1 = (SQLJoinTableSource) from;
//                from1.accept(new Scope(scope, from1));
//            } else if (from instanceof SQLUnionQueryTableSource) {
//                SQLUnionQueryTableSource from1 = (SQLUnionQueryTableSource) from;
//            } else if (from instanceof SQLSubqueryTableSource) {
//                SQLSubqueryTableSource from1 = (SQLSubqueryTableSource) from;
//            } else if (from instanceof SQLValuesTableSource) {
//                SQLValuesTableSource from1 = (SQLValuesTableSource) from;
//            }
//            System.out.println();
//        }
////        for (Scope child : ) {
////            visit(scope);
////        }
////        Map<? extends Class<? extends SQLName>, List<SQLName>> map = names.stream().map(i -> (SQLName) i)
////                .filter(i -> i.getResolvedColumn() != null)
////                .collect(Collectors.groupingBy(k -> k.getClass()));
////        List<SQLPropertyExpr> sqlNames1 = (List) map.getOrDefault(SQLPropertyExpr.class, Collections.emptyList());
////        List<SQLIdentifierExpr> sqlNames2 = (List) map.getOrDefault(SQLIdentifierExpr.class, Collections.emptyList());
//
////        Map<SQLTableSource, List<SQLPropertyExpr>> collect = sqlNames1.stream()
////                .filter(i -> i.getResolvedTableSource() != null).collect(Collectors.groupingBy(k -> k.getResolvedTableSource()));
////        Map<SQLTableSource, List<SQLPropertyExpr>>  collect2 = (List<SQLTableSource>) sqlNames2.stream()
////                .filter(i -> i.getResolvedTableSource() != null).collect(Collectors.groupingBy(k -> k.getResolvedTableSource()));
////        Stream.concat(sqlNames1.stream(),sqlNames2.stream()).map(i->i.getTA)
////        for (Scope child : scope.getChildren()) {
////            visit(scope);
////        }
//
//    }
//
    static interface Scope {
        void addSubQueries(Scope queryScope);

        void addSource(QueryScope queryScope);
    }

    @Getter
    static class LeftScope implements Scope {
        private QueryScope parent;
        SQLExprTableSource tableSource;

        public LeftScope(QueryScope queryScope, SQLExprTableSource tableSource) {
            this.parent = queryScope;
            this.tableSource = tableSource;
            queryScope.addSubQueries(this);
        }

        @Override
        public void addSubQueries(Scope queryScope) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addSource(QueryScope queryScope) {
            throw new UnsupportedOperationException();
        }
    }
//
//    static class JoinScope extends MySqlASTVisitorAdapter implements Scope {
//        private Scope parent;
//        SQLJoinTableSource tableSource;
//        private List<Scope> conditionSubQueries = new ArrayList<>();
//        private Scope left;
//
//        public JoinScope(QueryScope parent, SQLJoinTableSource tableSource) {
//            this.parent = parent;
//            this.tableSource = tableSource;
//        }
//        @Override
//        public void endVisit(SQLExprTableSource x) {
//            left = new LeftScope(this, x);
//        }
//
//
//        @Override
//        public boolean visit(SQLQueryExpr x) {
//            SQLSelect subQuery = x.getSubQuery();
//            QueryScope a = new QueryScope(this, subQuery.getQueryBlock());
//            subQuery.accept(a);
//            return false;
//        }
//
//        @Override
//        public void addSubQueries(Scope queryScope) {
//            conditionSubQueries.add(queryScope);
//        }
//
//        @Override
//        public void addSource(QueryScope queryScope) {
//
//        }
//    }

    @Getter
    static class QueryScope extends MySqlASTVisitorAdapter implements Scope {
        private Scope parent;
        private SQLObject ast;
        private List<Scope> children = new LinkedList<>();
        private List<SQLObject> names = new LinkedList<>();
        private List<Scope> source = new LinkedList<>();

        public QueryScope(Scope parent, SQLObject select) {
            this.parent = parent;
            this.ast = select;
            if (parent != null) {
                this.parent.addSubQueries(this);
            }
        }

        public QueryScope(Scope parent, com.alibaba.fastsql.sql.ast.statement.SQLTableSource select) {
            this.parent = parent;
            this.ast = select;
            if (parent != null) {
                this.parent.addSource(this);
            }
        }

        @Override
        public void endVisit(SQLSelectItem x) {
            String s = x.getAlias();
            if (s != null) {
                names.add(x);
            }
            super.endVisit(x);
        }

        @Override
        public boolean visit(SQLQueryExpr x) {
            SQLSelect subQuery = x.getSubQuery();
            QueryScope a = new QueryScope(this, subQuery.getFirstQueryBlock());
            subQuery.accept(a);
            return false;
        }


        @Override
        public boolean visit(SQLExprTableSource x) {
            boolean b = x.getExpr() instanceof SQLQueryExpr;
            if (!b) {
                new LeftScope(this, x);
            } else {
                QueryScope a = new QueryScope(this, x.getExpr());
                x.getExpr().accept(a);
            }
            return false;
        }

        @Override
        public boolean visit(SQLJoinTableSource x) {
            SQLExpr condition = x.getCondition();
            SQLTableSource left = x.getLeft();
            SQLTableSource right = x.getRight();
            QueryScope scope1 = new QueryScope(this, left);
            left.accept(scope1);
            QueryScope scope2 = new QueryScope(this, right);
            right.accept(scope2);
            if (condition != null) {
                condition.accept(new QueryScope(this, condition));
            }
            return false;
        }

        @Override
        public void endVisit(SQLIdentifierExpr x) {
            SQLColumnDefinition resolvedColumn = x.getResolvedColumn();
            if (resolvedColumn != null) {
                names.add(x);
            }
            super.endVisit(x);
        }

        @Override
        public void endVisit(SQLPropertyExpr x) {
            SQLColumnDefinition resolvedColumn = x.getResolvedColumn();
            if (resolvedColumn != null) {
                names.add(x);
            }
            super.endVisit(x);
        }

        @Override
        public void addSubQueries(Scope queryScope) {
            children.add(queryScope);
        }

        @Override
        public void addSource(QueryScope queryScope) {
            this.source.add(queryScope);
        }
    }
}