package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.FilterFunction;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageFilterFunction<T> implements FilterFunction<L3StreamTupleContainer<T>> {

  private final FilterFunction<T> delegate;

  public LineageFilterFunction(FilterFunction<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean filter(L3StreamTupleContainer<T> value) throws Exception {
    return delegate.filter(value.tuple());
  }
}
