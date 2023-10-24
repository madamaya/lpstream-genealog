package io.palyvos.provenance.usecases.linearroad.noprovenance;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate.VehicleAccumulator;
import io.palyvos.provenance.util.BaseAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class LinearRoadVehicleAggregateL3 implements
    AggregateFunction<LinearRoadInputTuple, LinearRoadVehicleAggregateL3.VehicleAccumulatorL3, VehicleTuple> {

  @Override
  public VehicleAccumulatorL3 createAccumulator() {
    return new VehicleAccumulatorL3();
  }

  @Override
  public VehicleAccumulatorL3 add(LinearRoadInputTuple tuple, VehicleAccumulatorL3 acc) {
    acc.add(tuple);
    return acc;
  }

  @Override
  public VehicleTuple getResult(VehicleAccumulatorL3 acc) {
    return acc.getAggregatedResult();
  }

  @Override
  public VehicleAccumulatorL3 merge(VehicleAccumulatorL3 a,
                                    VehicleAccumulatorL3 b) {
    return a.merge(b);
  }

  public static class VehicleAccumulatorL3 extends BaseAccumulator<LinearRoadInputTuple,
          VehicleTuple, VehicleAccumulatorL3> {

    private final Set<String> positions = new HashSet<>();
    private int latestXWay = -1;
    private int latestLane = -1;
    private int latestDir = -1;
    private int latestSeg = -1;
    private int latestPos = -1;
    private int counter;
    private String key;
    private long timestamp = -1;
    private long latestTimestamp = -1;

    @Override
    public void doAdd(LinearRoadInputTuple t) {
      counter++;
      if (latestTimestamp < t.getTimestamp()) {
        latestTimestamp = t.getTimestamp();
        latestXWay = t.getXway();
        latestLane = t.getLane();
        latestDir = t.getDir();
        latestSeg = t.getSeg();
        latestPos = t.getPos();
        key = t.getKey();
      }
      timestamp = Math.max(timestamp, t.getTimestamp());
      String posString = t.getXway() + "," + t.getLane() + "," + t.getDir() + ","
          + t.getSeg() + "," + t.getPos();
      positions.add(posString);
    }

    @Override
    public VehicleTuple doGetAggregatedResult() {
      boolean uniquePosition = positions.size() == 1;
      VehicleTuple result = new VehicleTuple(timestamp, Integer.valueOf(key),
          counter, latestXWay, latestLane, latestDir, latestSeg,
          latestPos, uniquePosition);
      return result;
    }

    @Override
    protected void doMerge(VehicleAccumulatorL3 other) {
      throw new UnsupportedOperationException("merge");
    }
  }

}
