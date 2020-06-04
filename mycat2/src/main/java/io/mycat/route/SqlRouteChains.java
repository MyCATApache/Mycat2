package io.mycat.route;

import com.google.common.collect.ImmutableList;

public enum SqlRouteChains {
    INSTANCE;
    final SqlRouteChain[] sqlRouteChains = ImmutableList.builder().add(new NoTablesRouter(), new GlobalRouter()).build().toArray(new SqlRouteChain[0]);

    public boolean execute(ParseContext context) {
        for (int i = 0; i < sqlRouteChains.length; i++) {
            boolean handle = sqlRouteChains[i].handle(context);
            if (!handle) {
            } else {
                return true;
            }
        }
        return false;
    }
}