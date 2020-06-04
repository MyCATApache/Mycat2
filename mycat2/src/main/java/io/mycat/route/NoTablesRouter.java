package io.mycat.route;

public class NoTablesRouter implements SqlRouteChain {
    @Override
    public boolean handle(ParseContext t) {
        if (t.getLeftTables().isEmpty()) {
            t.plan(HBTBuilder.create()
                    .from(t.getDefaultTarget(),
                            t.getSqlStatement().toString()).build());
            return true;
        }
        return false;
    }
}