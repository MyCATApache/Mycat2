package io.mycat.calcite.rewriter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
public class MycatDynamicParam implements Serializable {
    int index;

    public MycatDynamicParam(int index) {
        this.index = index;
    }
}
