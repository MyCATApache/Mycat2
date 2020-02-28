package io.mycat.calcite.prepare;

import java.util.Iterator;
import java.util.List;

public interface TextUpdateInfoProvider {
    Iterator apply(MycatTextUpdatePrepareObject prepareObject, List params);
}