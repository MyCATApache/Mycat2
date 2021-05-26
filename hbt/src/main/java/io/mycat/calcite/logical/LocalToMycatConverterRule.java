/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite.logical;

import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.ViewConvention;
import io.mycat.calcite.localrel.LocalConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class LocalToMycatConverterRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(MycatView.class, ViewConvention.INSTANCE,
          MycatConvention.INSTANCE, "CassandraToEnumerableConverterRule")
      .withRuleFactory(LocalToMycatConverterRule::new);

  /** Creates a CassandraToEnumerableConverterRule. */
  protected LocalToMycatConverterRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new LocalToMycatRelConverter(rel.getCluster(), newTraitSet, rel);
  }
}
