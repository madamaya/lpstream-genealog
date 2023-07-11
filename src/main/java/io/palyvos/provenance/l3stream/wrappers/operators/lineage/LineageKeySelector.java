package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.java.functions.KeySelector;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageKeySelector<IN, KEY> implements KeySelector<L3StreamTupleContainer<IN>, KEY> {

  private final KeySelector<IN, KEY> delegate;

  public LineageKeySelector(KeySelector<IN, KEY> delegate) {
    this.delegate = delegate;
  }

  @Override
  public KEY getKey(L3StreamTupleContainer<IN> value) throws Exception {
    return delegate.getKey(value.tuple());
  }
}
