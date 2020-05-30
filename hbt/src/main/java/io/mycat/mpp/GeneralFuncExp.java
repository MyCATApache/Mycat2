package io.mycat.mpp;

import io.mycat.queryCondition.SimpleColumnInfo;

import java.util.List;
import java.util.Map;

public  abstract class GeneralFuncExp extends FunctionExp{
        public GeneralFuncExp(String name, List<SqlValue> params) {
            super(name, params);
        }
        
        abstract SqlValue eval(Map<SimpleColumnInfo, Integer> colIndex, Row tuple);
    }