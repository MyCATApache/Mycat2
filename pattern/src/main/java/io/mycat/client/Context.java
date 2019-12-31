package io.mycat.client;

import io.mycat.EvalNodeVisitor2;
import io.mycat.describer.Describer;
import io.mycat.describer.ParseNode;
import io.mycat.wu.BaseQuery;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.*;

@AllArgsConstructor
@Getter
public class Context {
    final java.util.Map<String, Collection<String>> tables;
    final Map<String, String> names;
    final java.util.Map<String, String> tags;
    final String type;
    final String explain;
}
