package io.mycat.calcite.spm;

import io.mycat.calcite.CodeExecuterContext;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class PlanResultSet {
   final Long baselineId;
   final boolean ok;
   final CodeExecuterContext context;
}
