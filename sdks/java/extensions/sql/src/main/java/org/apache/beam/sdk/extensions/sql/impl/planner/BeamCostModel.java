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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import java.util.Objects;

/**
 * <code>VolcanoCost</code> represents the cost of a plan node.
 *
 * <p>This class is immutable: none of the methods modify any member variables.
 */
public class BeamCostModel implements RelOptCost {
  // ~ Static fields/initializers ---------------------------------------------

  static final BeamCostModel INFINITY =
      new BeamCostModel(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        @Override
        public String toString() {
          return "{inf}";
        }
      };

  static final BeamCostModel HUGE =
      new BeamCostModel(
          Double.MAX_VALUE,
          Double.MAX_VALUE,
          Double.MAX_VALUE,
          Double.MAX_VALUE,
          Double.MAX_VALUE) {
        @Override
        public String toString() {
          return "{huge}";
        }
      };

  static final BeamCostModel ZERO =
      new BeamCostModel(0.0, 0.0, 0.0, 0.0, 0.0) {
        @Override
        public String toString() {
          return "{0}";
        }
      };

  static final BeamCostModel TINY =
      new BeamCostModel(1.0, 0.001, 1.0, 1.0, 0.001) {
        @Override
        public String toString() {
          return "{tiny}";
        }
      };

  public static final BeamCostModel.Factory FACTORY = new BeamCostModel.Factory();

  // ~ Instance fields --------------------------------------------------------

  final double cpu;
  final double cpuRate;
  final double rowCount;
  final double rate;
  final double window;

  BeamCostModel(double rowCount, double rate, double window, double cpu, double cpuRate) {
    this.rowCount = Math.max(rowCount, 0);
    this.cpu = Math.max(cpu, 0);
    this.cpuRate = Math.max(cpuRate, 0);
    this.rate = Math.max(rate, 0);
    this.window = Math.max(window, 0);
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public double getCpu() {
    return cpu;
  }

  @Override
  public boolean isInfinite() {
    return (this.equals(INFINITY))
        || (this.rowCount == Double.POSITIVE_INFINITY)
        || (this.cpu == Double.POSITIVE_INFINITY)
        || (this.rate == Double.POSITIVE_INFINITY)
        || (this.window == Double.POSITIVE_INFINITY)
        || (this.cpuRate == Double.POSITIVE_INFINITY);
  }

  @Override
  public double getIo() {
    return 0;
  }

  public double getCpuRate() {
    return cpuRate;
  }

  @Override
  public boolean isLe(RelOptCost other) {
    BeamCostModel that = (BeamCostModel) other;
    // This if is to make sure Infinity.isLe(Huge) wont be true.
    // Without this both of the costCombinations are infinity and therefore, this will return true.
    if (getCostCombination(this) == Double.POSITIVE_INFINITY
        || getCostCombination(that) == Double.POSITIVE_INFINITY)
      return this.cpuRate < that.cpuRate
          || (this.rate == that.rate && this.rowCount <= that.rowCount);
    return getCostCombination(this) <= getCostCombination(that);
  }

  @Override
  public boolean isLt(RelOptCost other) {
    BeamCostModel that = (BeamCostModel) other;
    // This is to make sure Huge.isLt(Infinity) returns true
    if (that.isInfinite()) return !this.isInfinite();
    return getCostCombination(this) < getCostCombination(that);
  }

  private static double getCostCombination(BeamCostModel cost) {
    return cost.cpu + cost.cpuRate * 3600;
  }

  @Override
  public double getRows() {
    return rowCount;
  }

  public double getRate() {
    return rate;
  }

  public double getWindow() {
    return window;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowCount, cpu, cpuRate, window, rate);
  }

  @SuppressWarnings("NonOverridingEquals")
  @Override
  public boolean equals(RelOptCost other) {
    return other instanceof BeamCostModel
        && (this.rowCount == ((BeamCostModel) other).rowCount)
        && (this.cpu == ((BeamCostModel) other).cpu)
        && (this.cpuRate == ((BeamCostModel) other).cpuRate)
        && (this.rate == ((BeamCostModel) other).rate)
        && (this.window == ((BeamCostModel) other).window);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BeamCostModel) {
      return equals((BeamCostModel) obj);
    }
    return false;
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof BeamCostModel)) {
      return false;
    }
    BeamCostModel that = (BeamCostModel) other;
    return ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
            && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
            && (Math.abs(this.cpuRate - that.cpuRate) < RelOptUtil.EPSILON))
        && (Math.abs(this.rate - that.rate) < RelOptUtil.EPSILON)
        && (Math.abs(this.window - that.window) < RelOptUtil.EPSILON);
  }

  @Override
  public BeamCostModel minus(RelOptCost other) {
    if (this.equals(INFINITY)) {
      return this;
    }
    BeamCostModel that = (BeamCostModel) other;
    return new BeamCostModel(
        this.rowCount - that.rowCount,
        this.rate - that.rate,
        this.window - that.window,
        this.cpu - that.cpu,
        this.cpuRate - that.cpuRate);
  }

  @Override
  public BeamCostModel multiplyBy(double factor) {
    if (this.equals(INFINITY)) {
      return this;
    }
    return new BeamCostModel(
        rowCount * factor, rate * factor, window * factor, cpu * factor, cpuRate * factor);
  }

  @Override
  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite. (Except the window size)
    BeamCostModel that = (BeamCostModel) cost;
    double d = 1;
    double n = 0;
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.cpuRate != 0)
        && !Double.isInfinite(this.cpuRate)
        && (that.cpuRate != 0)
        && !Double.isInfinite(that.cpuRate)) {
      d *= this.cpuRate / that.cpuRate;
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  @Override
  public BeamCostModel plus(RelOptCost other) {
    BeamCostModel that = (BeamCostModel) other;
    if (this.equals(INFINITY) || that.equals(INFINITY)) {
      return INFINITY;
    }
    return new BeamCostModel(
        this.rowCount + that.rowCount,
        this.rate + that.rate,
        this.window + that.window,
        this.cpu + that.cpu,
        this.cpuRate + that.cpuRate);
  }

  @Override
  public String toString() {
    return "{"
        + rowCount
        + " rows, "
        + cpu
        + " cpu, "
        + cpuRate
        + " cpuRate, "
        + rate
        + " rate, "
        + window
        + " window}";
  }

  public static BeamCostModel convertRelOptCost(RelOptCost ic) {
    BeamCostModel inputCost;
    if (ic instanceof BeamCostModel) {
      inputCost = ((BeamCostModel) ic);
    } else {
      inputCost = BeamCostModel.FACTORY.makeCost(ic.getRows(), ic.getCpu(), ic.getIo());
    }
    return inputCost;
  }

  /**
   * Implementation of {@link org.apache.calcite.plan.RelOptCostFactory} that creates {@link
   * BeamCostModel}s.
   */
  public static class Factory implements RelOptCostFactory {

    @Override
    public BeamCostModel makeCost(double dRows, double dCpu, double dIo) {
      return makeInfiniteCost();
    }

    public BeamCostModel makeCost(
        double dRows, double dRate, double dWindow, double dCpu, double dCpuRate) {
      return new BeamCostModel(dRows, dRate, dWindow, dCpu, dCpuRate);
    }

    @Override
    public BeamCostModel makeHugeCost() {
      return BeamCostModel.HUGE;
    }

    @Override
    public BeamCostModel makeInfiniteCost() {
      return BeamCostModel.INFINITY;
    }

    @Override
    public BeamCostModel makeTinyCost() {
      return BeamCostModel.TINY;
    }

    @Override
    public BeamCostModel makeZeroCost() {
      return BeamCostModel.ZERO;
    }
  }
}
