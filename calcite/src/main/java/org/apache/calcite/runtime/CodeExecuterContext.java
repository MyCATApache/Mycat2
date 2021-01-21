package org.apache.calcite.runtime;

import lombok.Getter;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Getter
public class CodeExecuterContext {
    private List<RelNode> mycatViews;
    final Map<String, Object> context;
    final ArrayBindable bindable;
    final String code;

    public CodeExecuterContext(List<RelNode> mycatViews, Map<String, Object> context, ArrayBindable bindable, String code) {
        this.mycatViews = mycatViews;
        this.context = context;
        this.bindable = bindable;

//                new ArrayBindable() {
//            @Override
//            public Class<Object[]> getElementType() {
//                return Object[].class;
//            }
//
//            @Override
//            public Enumerable<Object[]> bind(NewMycatDataContext dataContext) {
//                return CodeExecuterContext.bind(dataContext);
//            }
//        };
        this.code = code;
    }

    public static final CodeExecuterContext of(List<RelNode> mycatViews, Map<String, Object> context,
                                               ArrayBindable bindable,
                                               String code) {
        return new CodeExecuterContext(mycatViews,context, bindable, code);
    }

    public Class getElementType() {
        return java.lang.Object[].class;
    }


}
