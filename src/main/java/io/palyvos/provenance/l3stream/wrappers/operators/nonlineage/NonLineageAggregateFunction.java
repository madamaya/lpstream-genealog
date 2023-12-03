package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.function.Supplier;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageAggregateFunction<IN, ACC, OUT>
    implements AggregateFunction<
        L3StreamTupleContainer<IN>, GenealogAccumulator<ACC>, L3StreamTupleContainer<OUT>> {

  private final AggregateFunction<IN, ACC, OUT> delegate;
  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public NonLineageAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> strategySupplier,
      AggregateFunction<IN, ACC, OUT> delegate) {
    this.delegate = delegate;
    this.strategySupplier = strategySupplier;
  }

  /*
   * CNFM: lineageReliableFlagについてメモ．
   * CNFM: 今のやり方だと『Stateは作られたけど1レコードも到着しないうちにCheckpointだけ取られた』というケースでもlineageReliableFlagはFalseになる．
   * CNFM: （実際の実行時にそのようなことがあるのかは不明）
   * CNFM: これを避けるために，通常実行時もtrueで初期化して，addの中でレコードが処理されたらfalseに書き換えるというやり方がある．
   * CNFM: ただしaddのたびにfalseの代入をすることになるから，性能に影響するかもしれないから初期の実装では今のやり方を採用．
   */
  @Override
  public GenealogAccumulator<ACC> createAccumulator() {
    return new GenealogAccumulator<>(strategySupplier.get(), delegate.createAccumulator(), false);
  }

  @Override
  public GenealogAccumulator<ACC> add(
      L3StreamTupleContainer<IN> value, GenealogAccumulator<ACC> accumulator) {
    // accumulator.strategy.addWindowProvenance(value);
    accumulator.updateTimestamp(value.getTimestamp());
    if (accumulator.getStimulusList() == null || accumulator.getStimulusList().get(0) < value.getStimulusList().get(0)) {
      accumulator.setStimulus(System.currentTimeMillis());
      accumulator.setStimulusList(value.getStimulusList());
    }
    accumulator.setAccumulator(delegate.add(value.tuple(), accumulator.getAccumulator()));
    return accumulator;
  }

  @Override
  public L3StreamTupleContainer<OUT> getResult(GenealogAccumulator<ACC> accumulator) {
    long ts = System.currentTimeMillis();
    OUT result = delegate.getResult(accumulator.getAccumulator());
    L3StreamTupleContainer<OUT> genealogResult = new L3StreamTupleContainer<>(result);
    // accumulator.strategy.annotateWindowResult(genealogResult);
    genealogResult.setTimestamp(accumulator.getTimestamp());
    //genealogResult.setStimulus(accumulator.getStimulus());
    genealogResult.setStimulusList(accumulator.getStimulusList());
    genealogResult.setStimulusList(accumulator.getStimulus());
    genealogResult.setStimulusList(ts);
    return genealogResult;
  }

  @Override
  public GenealogAccumulator<ACC> merge(
      GenealogAccumulator<ACC> a, GenealogAccumulator<ACC> b) {
    throw new UnsupportedOperationException("merge");
  }
}
