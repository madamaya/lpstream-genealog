package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageRichMapFunction<T, O>
    extends RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> {

  private final RichMapFunction<T, O> delegate;

  public NonLineageRichMapFunction(RichMapFunction<T, O> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.delegate.setRuntimeContext(getRuntimeContext());
    this.delegate.open(parameters);
  }

  @Override
  public L3StreamTupleContainer<O> map(L3StreamTupleContainer<T> value) throws Exception {
    O result = delegate.map(value.tuple());
    L3StreamTupleContainer<O> genealogResult = new L3StreamTupleContainer<>(result);
    // GenealogMapHelper.INSTANCE.annotateResult(value, genealogResult);
    genealogResult.copyTimesTFL(value);
    return genealogResult;
  }

  @Override
  public void close() throws Exception {
    this.delegate.close();
  }
}
