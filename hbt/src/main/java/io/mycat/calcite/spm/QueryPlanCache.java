/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.spm;

import io.mycat.DrdsSql;
import io.mycat.calcite.CodeExecuterContext;

import java.util.List;

public interface QueryPlanCache {

    public void init();

    public void delete(List<String> uniqueTables);

    public Baseline getBaseline(DrdsSql baseLineSql);

    public PlanResultSet saveBaselinePlan(boolean fix, boolean complex, Baseline baseline, BaselinePlan newBaselinePlan);

    public List<CodeExecuterContext> getAcceptedMycatRelList(DrdsSql baselineSql);

    public PlanResultSet add(boolean fix, DrdsSql drdsSql);

    public List<Baseline> list();

    public void clearCache();

    void loadBaseline(long value);

    void loadPlan(long value);

    void persistPlan(long value);

    void clearBaseline(long value);

    void clearPlan(long value);

    void deleteBaseline(long value);

    void deletePlan(long value);

    public void saveBaselines();

    public void unFix(long baselineId);

    public Baseline getBaseline(long baselineId);

    public void persistBaseline(long baselineId);
}