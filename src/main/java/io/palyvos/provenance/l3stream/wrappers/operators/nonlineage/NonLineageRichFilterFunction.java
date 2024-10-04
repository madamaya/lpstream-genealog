package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageRichFilterFunction<T> extends RichFilterFunction<L3StreamTupleContainer<T>> {

  private final RichFilterFunction<T> delegate;

  public NonLineageRichFilterFunction(RichFilterFunction<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    delegate.setRuntimeContext(getRuntimeContext());
    delegate.open(parameters);
  }

  @Override
  public boolean filter(L3StreamTupleContainer<T> value) throws Exception {
    return delegate.filter(value.tuple());
  }

  @Override
  public void close() throws Exception {
    super.close();
    delegate.close();
  }
}
