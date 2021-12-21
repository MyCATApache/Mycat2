package io.ordinate.engine.factory;

import io.ordinate.engine.physicalplan.PhysicalPlan;

public interface Factory {
    PhysicalPlan create(ComplierContext context);
}
