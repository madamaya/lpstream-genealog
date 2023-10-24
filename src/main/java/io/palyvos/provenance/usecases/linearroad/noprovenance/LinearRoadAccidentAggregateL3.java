package io.palyvos.provenance.usecases.linearroad.noprovenance;

import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate.AccidentCountAccumulator;
import io.palyvos.provenance.util.BaseAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class LinearRoadAccidentAggregateL3 implements AggregateFunction<VehicleTuple,
        LinearRoadAccidentAggregateL3.AccidentCountAccumulatorL3, CountTuple> {

  @Override
  public AccidentCountAccumulatorL3 createAccumulator() {
    return new AccidentCountAccumulatorL3();
  }

  @Override
  public AccidentCountAccumulatorL3 add(VehicleTuple value, AccidentCountAccumulatorL3 accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public CountTuple getResult(AccidentCountAccumulatorL3 accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public AccidentCountAccumulatorL3 merge(AccidentCountAccumulatorL3 a, AccidentCountAccumulatorL3 b) {
    return a.merge(b);
  }

  public static class AccidentCountAccumulatorL3 extends BaseAccumulator<VehicleTuple,
      CountTuple, AccidentCountAccumulatorL3> {

    private final transient Set<Integer> carIds = new HashSet<>();
    private long timestamp = -1;
    private String key;

    @Override
    public void doAdd(VehicleTuple tuple) {
      timestamp = Math.max(timestamp, tuple.getTimestamp());
      key = tuple.getKey();
      carIds.add(tuple.getVid());
    }

    @Override
    public CountTuple doGetAggregatedResult() {
      return new CountTuple(timestamp, key, carIds.size());
    }

    @Override
    protected void doMerge(AccidentCountAccumulatorL3 other) {
      throw new UnsupportedOperationException("merge");
    }
  }

}
