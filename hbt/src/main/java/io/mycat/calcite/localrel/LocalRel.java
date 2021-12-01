package io.mycat.calcite.localrel;

import io.mycat.beans.mycat.MycatRelDataType;
import org.apache.calcite.rel.RelNode;

import java.io.Serializable;

public interface LocalRel extends RelNode, Serializable {
    MycatRelDataType getMycatRelDataType();
}
