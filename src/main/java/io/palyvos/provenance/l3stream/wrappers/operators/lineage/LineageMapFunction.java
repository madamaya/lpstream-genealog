package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.MapFunction;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageMapFunction<T, O>
    implements MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> {

  private final MapFunction<T, O> delegate;

  public LineageMapFunction(MapFunction<T, O> delegate) {
    this.delegate = delegate;
  }

  @Override
  public L3StreamTupleContainer<O> map(L3StreamTupleContainer<T> value) throws Exception {
    long ts = System.currentTimeMillis();
    O result = delegate.map(value.tuple());
    L3StreamTupleContainer<O> genealogResult = new L3StreamTupleContainer<>(result);
    GenealogMapHelper.INSTANCE.annotateResult(value, genealogResult);
    genealogResult.setLineageReliable(value.getLineageReliable());
    // genealogResult.copyTimes(value);
    genealogResult.setTimestamp(value.getTimestamp());
    genealogResult.setStimulusList(value.getStimulusList());
    genealogResult.setStimulusList(ts);
    genealogResult.setPartitionId(value.getPartitionId());
    return genealogResult;
  }
}
