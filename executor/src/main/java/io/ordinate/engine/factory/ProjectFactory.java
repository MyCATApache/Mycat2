package io.ordinate.engine.factory;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.physical.MycatProject;
import io.ordinate.engine.builder.ExecuteCompiler;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.physicalplan.ProjectionPlan;
import org.apache.calcite.rex.RexNode;

public class ProjectFactory implements Factory{
    private  MycatProject mycatRel;
    private  Factory inputFactory;

    public ProjectFactory(MycatProject mycatRel, Factory inputFactory) {
        this.mycatRel = mycatRel;
        this.inputFactory = inputFactory;
    }

    public static ProjectFactory of(MycatProject mycatRel, Factory inputFactory) {
        return new ProjectFactory(mycatRel,inputFactory);
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        PhysicalPlan physicalPlan = inputFactory.create(context);
        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        for (RexNode project : mycatRel.getProjects()) {
            Function function = context.convertRex(project);
            builder.add(function);
        }
        return ExecuteCompiler.project(physicalPlan, builder.build());
    }
}
