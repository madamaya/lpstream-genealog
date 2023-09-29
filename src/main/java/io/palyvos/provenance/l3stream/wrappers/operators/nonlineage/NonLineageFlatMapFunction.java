package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.utils.NonLineageCollectorAdapter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageFlatMapFunction<T, O>
    implements FlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> {

  private final FlatMapFunction<T, O> delegate;

   public NonLineageFlatMapFunction(FlatMapFunction<T, O> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void flatMap(L3StreamTupleContainer<T> value, Collector<L3StreamTupleContainer<O>> out)
      throws Exception {
    delegate.flatMap(value.tuple(), new NonLineageCollectorAdapter<>(value, out));
  }
}
