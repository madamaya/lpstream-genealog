package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.function.Supplier;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageAggregateFunction<IN, ACC, OUT>
    implements AggregateFunction<
        L3StreamTupleContainer<IN>, GenealogAccumulator<ACC>, L3StreamTupleContainer<OUT>> {

  private final AggregateFunction<IN, ACC, OUT> delegate;
  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public LineageAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> strategySupplier,
      AggregateFunction<IN, ACC, OUT> delegate) {
    this.delegate = delegate;
    this.strategySupplier = strategySupplier;
  }

  @Override
  public GenealogAccumulator<ACC> createAccumulator() {
    return new GenealogAccumulator<>(strategySupplier.get(), delegate.createAccumulator(), true);
  }

  @Override
  public GenealogAccumulator<ACC> add(
      L3StreamTupleContainer<IN> value, GenealogAccumulator<ACC> accumulator) {
    accumulator.getStrategy().addWindowProvenance(value);
    accumulator.updateLineageReliable(value.getLineageReliable());
    accumulator.updateTimestamp(value.getTimestamp());
    // accumulator.updateStimulus(value.getStimulus());
    accumulator.setTfl(value.getTfl());
    accumulator.setAccumulator(delegate.add(value.tuple(), accumulator.getAccumulator()));
    return accumulator;
  }

  @Override
  public L3StreamTupleContainer<OUT> getResult(GenealogAccumulator<ACC> accumulator) {
    OUT result = delegate.getResult(accumulator.getAccumulator());
    L3StreamTupleContainer<OUT> genealogResult = new L3StreamTupleContainer<>(result);
    accumulator.getStrategy().annotateWindowResult(genealogResult);
    genealogResult.setLineageReliable(accumulator.isLineageReliable());
    genealogResult.setTimestamp(accumulator.getTimestamp());
    genealogResult.setTfl(accumulator.getTfl());
    return genealogResult;
  }

  @Override
  public GenealogAccumulator<ACC> merge(
      GenealogAccumulator<ACC> a, GenealogAccumulator<ACC> b) {
    throw new UnsupportedOperationException("merge");
  }
}
