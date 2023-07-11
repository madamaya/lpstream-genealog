package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageSinkFunction<IN> implements
    SinkFunction<L3StreamTupleContainer<IN>> {

  private final SinkFunction<IN> delegate;

  public NonLineageSinkFunction(SinkFunction<IN> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void invoke(L3StreamTupleContainer<IN> value, Context context) throws Exception {
    delegate.invoke(value.tuple(), context);
  }
}
