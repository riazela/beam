/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

/**
 * {@code BeamRelNode} to replace a {@code Minus} node.
 *
 * <p>Corresponds to the SQL {@code EXCEPT} operator.
 */
public class BeamMinusRel extends Minus implements BeamRelNode {

  public BeamMinusRel(
      RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new BeamMinusRel(getCluster(), traitSet, inputs, all);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
    final List<RelNode> inputs = this.getInputs();
    BeamCostModel dRows =
        BeamCostModel.convertRelOptCost(
            mq.getNonCumulativeCost(BeamSqlRelUtils.getInput(inputs.get(0))));
    double rowsSum = dRows.getRows();
    double rateSum = dRows.getRate();
    for (int i = 1; i < inputs.size(); i++) {
      BeamCostModel t =
          BeamCostModel.convertRelOptCost(
              mq.getNonCumulativeCost(BeamSqlRelUtils.getInput(inputs.get(i))));
      dRows = dRows.minus(t.multiplyBy(0.5));
      rowsSum += t.getRows();
      rateSum += t.getRate();
    }

    return BeamCostModel.FACTORY.makeCost(
        dRows.getRows(), dRows.getRate(), dRows.getWindow(), rowsSum, rateSum);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new BeamSetOperatorRelBase(this, BeamSetOperatorRelBase.OpType.MINUS, all);
  }
}
