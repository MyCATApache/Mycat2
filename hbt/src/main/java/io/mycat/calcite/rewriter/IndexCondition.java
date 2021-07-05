package io.mycat.calcite.rewriter;

import io.mycat.querycondition.QueryType;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;

@AllArgsConstructor
@Getter
@ToString
public class IndexCondition implements Comparable<IndexCondition>, Serializable {
    String indexName;
    String indexColumnName;
    QueryType queryType;

    public String toJson() {
        Map<String, String> map = new HashMap<>();
        map.put("indexName", JsonUtil.toJson(indexName));
        map.put("indexColumnNames", JsonUtil.toJson(indexColumnName));
        map.put("queryType", JsonUtil.toJson(queryType));
        return JsonUtil.toJson(map);
    }

    public static IndexCondition EMPTY = create( null, null);

    public IndexCondition(String indexName, String indexColumnNames) {
        this.indexName = indexName;
        this.indexColumnName = indexColumnNames;
    }


    public static IndexCondition create(String indexName, String indexColumnNames) {
        return new IndexCondition(indexName, indexColumnNames);
    }

    @Override
    public int compareTo(@NotNull IndexCondition o) {
        return this.queryType.compareTo(o.queryType);
    }

    public boolean canPushDown() {
        return queryType != null;
    }

    public String getName() {
        return indexName;
    }

    public IndexCondition withQueryType(QueryType queryType) {
        this.queryType = queryType;
        return this;
    }
    public QueryType getQueryType() {
        return queryType == null ? QueryType.PK_FULL_SCAN : queryType;
    }


}
