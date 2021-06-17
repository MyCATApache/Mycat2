package io.mycat.calcite;

import com.google.common.collect.ImmutableMap;
import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

@Getter
public class RelNodeContext {
    final RelRoot root;
    final SqlToRelConverter sqlToRelConverter;
    final SqlValidator validator;
    final RelBuilder relBuilder;
    final CalciteCatalogReader catalogReader;
    final RelDataType parameterRowType;
    final ImmutableMap<RexNode, RexNode> constantMap;

   public static final Logger LOGGER = LoggerFactory.getLogger(RelNodeContext.class);

    public RelNodeContext(RelRoot root, SqlToRelConverter sqlToRelConverter, SqlValidator validator, RelBuilder relBuilder, CalciteCatalogReader catalogReader, RelDataType parameterRowType) {
        this.root = root;
        this.sqlToRelConverter = sqlToRelConverter;
        this.validator = validator;
        this.relBuilder = relBuilder;
        this.catalogReader = catalogReader;
        this.parameterRowType = parameterRowType;

        RelNode relNode = root.rel;
        RelOptCluster cluster = relNode.getCluster();
        RelMetadataQuery metadataQuery = cluster.getMetadataQuery();
        RelOptPredicateList pulledUpPredicates = metadataQuery.getPulledUpPredicates(root.rel);
        this.constantMap = Optional.ofNullable(pulledUpPredicates).map(i -> {
            ImmutableMap.Builder<RexNode, RexNode> builder = ImmutableMap.builder();
            for (Map.Entry<RexNode, RexNode> rexNodeRexNodeEntry : i.constantMap.entrySet()) {
                RexNode key = rexNodeRexNodeEntry.getKey();
                RexNode value = rexNodeRexNodeEntry.getValue();
                try {
                    if (key instanceof RexSlot) {
                        RelColumnOrigin columnOrigin = metadataQuery.getColumnOrigin(relNode, ((RexSlot) key).getIndex());
                        if (columnOrigin != null && !columnOrigin.isDerived()) {
                            RelOptTable originTable = columnOrigin.getOriginTable();
                            int originColumnOrdinal = columnOrigin.getOriginColumnOrdinal();
                            RelDataType type = originTable.getRowType().getFieldList().get(originColumnOrdinal).getType();
                            RexTableInputRef inputRef = RexTableInputRef.of(RexTableInputRef.RelTableRef.of(originTable, 0), originColumnOrdinal, type);
                            builder.put(inputRef, value);
                        }
                    }
                }catch (Throwable throwable){
                    LOGGER.warn("",throwable);
                }
            }
            return builder.build();
        }).orElse(ImmutableMap.of());
    }
}
