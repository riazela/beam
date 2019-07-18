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
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

/**
 * {@code BeamRelNode} to replace a {@code Intersect} node.
 *
 * <p>This is used to combine two SELECT statements, but returns rows only from the first SELECT
 * statement that are identical to a row in the second SELECT statement.
 */
public class BeamIntersectRel extends Intersect implements BeamRelNode {
  public BeamIntersectRel(
      RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
    double dRows = Double.MAX_VALUE;
    for (RelNode input : inputs) {
      dRows = Math.min(dRows, mq.getRowCount(BeamSqlRelUtils.getBeamRelInput(input)));
    }
    dRows *= dRows;
    return dRows;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // similar to estimateRowCount
    Double minimumRows = Double.POSITIVE_INFINITY;
    Double minimumWindowSize = Double.POSITIVE_INFINITY;
    Double maximumWindowSize = 0d;
    Double cpuCost = 0d;
    Double rateSummation = 0d;
    for (RelNode input : inputs) {
      BeamCostModel inpCost =
          BeamCostModel.convertRelOptCost(mq.getNonCumulativeCost(BeamSqlRelUtils.getInput(input)));
      cpuCost += inpCost.getRows();
      rateSummation += inpCost.getRate();
      minimumRows = Math.min(minimumRows, inpCost.getRows());
      maximumWindowSize = Math.max(maximumWindowSize, inpCost.getWindow());
    }

    return BeamCostModel.FACTORY.makeCost(
        minimumRows, minimumWindowSize * rateSummation, minimumWindowSize, cpuCost, rateSummation);
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new BeamIntersectRel(getCluster(), traitSet, inputs, all);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new BeamSetOperatorRelBase(this, BeamSetOperatorRelBase.OpType.INTERSECT, all);
  }
}
