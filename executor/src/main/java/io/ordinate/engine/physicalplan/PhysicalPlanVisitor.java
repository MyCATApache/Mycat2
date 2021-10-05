/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.physicalplan;

public interface PhysicalPlanVisitor {
    void visit(CorrelateJoinPlan correlateJoin);

    void visit(CsvScanPlan csvScan);

    void visit(FilterPlan filter);

    void visit(NLJoinPlan nlJoin);

    void visit(ValuesPlan values);

    void visit(GroupByKeyPlan groupByKey);

    void visit(CalcPlan calc);

    void visit(NoKeysAggPlan noKeysAgg);

    void visit(UnionPlan union);

    void visit(OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan);

    void visit(DistinctPlan distinct);

    void visit(GroupByKeyWithAggPlan groupByKeyWithAgg);

    void visit(ProjectionPlan projection);

//    void visit(HashAgg hashAgg);

    void visit(LimitPlan limit);

    void visit(SortPlan sort);

    void visit(HeapTopNPlan topNPlan);
}
