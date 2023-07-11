package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageFlatMapFunction<T, O>
    implements FlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> {

  private final FlatMapFunction<T, O> delegate;

  private static class CollectorAdapter<T, O> implements Collector<O> {

    private final L3StreamTupleContainer<T> input;
    private final Collector<L3StreamTupleContainer<O>> delegate;

    public CollectorAdapter(
        L3StreamTupleContainer<T> input, Collector<L3StreamTupleContainer<O>> delegate) {
      this.input = input;
      this.delegate = delegate;
    }

    @Override
    public void collect(O record) {
      L3StreamTupleContainer<O> genealogResult = new L3StreamTupleContainer<>(record);
      GenealogMapHelper.INSTANCE.annotateResult(input, genealogResult);
      genealogResult.setLineageReliable(input.getLineageReliable());
      genealogResult.copyTimes(input);
      delegate.collect(genealogResult);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  public LineageFlatMapFunction(FlatMapFunction<T, O> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void flatMap(L3StreamTupleContainer<T> value, Collector<L3StreamTupleContainer<O>> out)
      throws Exception {
    delegate.flatMap(value.tuple(), new CollectorAdapter<>(value, out));
  }
}
