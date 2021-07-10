package io.mycat.calcite.rewriter;

import io.mycat.querycondition.QueryType;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;

@AllArgsConstructor
@Getter
@ToString
public class IndexCondition implements Comparable<IndexCondition>, Serializable {
    String indexName;
    List<String> indexColumnNames;
    QueryType queryType;
    List<RexNode> pushDownRexNodeList;
    List<RexNode> remainderRexNodeList;

    public String toJson() {
        Map<String, String> map = new HashMap<>();
        map.put("indexName", JsonUtil.toJson(indexName));
        map.put("indexColumnNames", JsonUtil.toJson(indexColumnNames));
        map.put("queryType", JsonUtil.toJson(queryType));
        return JsonUtil.toJson(map);
    }

    public static IndexCondition EMPTY = create(null, null,Collections.emptyList(),Collections.emptyList());

    public IndexCondition(String indexName,
                          List<String> indexColumnNames,
                          List<RexNode> pushDownRexNodeList,
                          List<RexNode> remainderRexNodeList) {
        this.indexName = indexName;
        this.indexColumnNames = indexColumnNames;
        this.pushDownRexNodeList = pushDownRexNodeList;
        this.remainderRexNodeList = remainderRexNodeList;
    }


    public static IndexCondition create(String indexName,
                                        List<String>  indexColumnNames,
                                        List<RexNode> pushDownRexNodeList,
                                        List<RexNode> remainderRexNodeList) {
        return new IndexCondition(indexName, indexColumnNames,pushDownRexNodeList,remainderRexNodeList);
    }

    @Override
    public int compareTo(@NotNull IndexCondition o) {
        return this.queryType.compareTo(o.queryType);
    }

    public boolean canPushDown() {
        return  queryType!=null;
    }

    public String getName() {
        return indexName;
    }

    public IndexCondition withQueryType(QueryType queryType) {
        this.queryType = queryType;
        return this;
    }

    public QueryType getQueryType() {
        return queryType;
    }


}
