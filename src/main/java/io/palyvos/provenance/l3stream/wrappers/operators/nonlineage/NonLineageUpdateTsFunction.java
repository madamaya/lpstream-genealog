package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.function.Function;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageUpdateTsFunction<T>
    implements MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> {

  private final Function<L3StreamTupleContainer<T>, Long> tsUpdateFunction;

  public NonLineageUpdateTsFunction(Function<L3StreamTupleContainer<T>, Long> tsUpdateFunction) {
    this.tsUpdateFunction = tsUpdateFunction;
  }

  @Override
  public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
    L3StreamTupleContainer<T> genealogResult = new L3StreamTupleContainer<>(value);
    genealogResult.copyTimes(value);
    genealogResult.setTimestamp(tsUpdateFunction.apply(value));
    return genealogResult;
  }
}
