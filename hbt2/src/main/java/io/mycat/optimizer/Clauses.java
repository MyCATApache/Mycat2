package io.mycat.optimizer;

import java.util.HashMap;

public class Clauses {

    /** Clauses in a SQL query. Ordered by evaluation order.
     * SELECT is set only when there is a NON-TRIVIAL SELECT clause. */
    public enum Clause {
        FROM, WHERE, GROUP_BY, HAVING, SELECT, SET_OP, ORDER_BY, FETCH, OFFSET
    }

    public static void main(String[] args) {
        //子查询列表
        //Sort=> 产生子查询:PROJECT,SORT,FILTER,JOIN,AGG =>any
        //Set=> 产生子查询:PROJECT,SORT,FILTER,JOIN,AGG =>any
        //AGG=> 不产生子查询:Project,Set,FILTER,Sort 产生子查询JOIN,AGG
        //Project=>不产生子查询:Project,Set,AGG,Sort 产生子查询:FILTER,JOIN
        //JOIN=>不产生子查询:Project,Set,AGG,FILTER,JOIN,Sort
        //FILTER=>不产生子查询:Project,Set,AGG,Sort 产生子查询:FILTER,JOIN
        //Correlate=>不下推
    }
}