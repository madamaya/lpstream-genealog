package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogJoinHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.JoinFunction;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageJoinFunction<IN1, IN2, OUT>
    implements JoinFunction<
        L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> {

  private final JoinFunction<IN1, IN2, OUT> delegate;

  public LineageJoinFunction(JoinFunction<IN1, IN2, OUT> delegate) {
    this.delegate = delegate;
  }

  @Override
  public L3StreamTupleContainer<OUT> join(
      L3StreamTupleContainer<IN1> first, L3StreamTupleContainer<IN2> second) throws Exception {
    long ts = System.currentTimeMillis();
    OUT result = delegate.join(first.tuple(), second.tuple());
    L3StreamTupleContainer<OUT> genealogResult = new L3StreamTupleContainer<>(result);
    GenealogJoinHelper.INSTANCE.annotateResult(first, second, genealogResult);
    genealogResult.setLineageReliable(first.getGenealogData() != null && second.getGenealogData() != null && first.getLineageReliable() && second.getLineageReliable());
    // genealogResult.copyTimes(first, second);
    if (first.getTimestamp() >= second.getTimestamp()) {
      genealogResult.setTimestamp(first.getTimestamp());
    } else {
      genealogResult.setTimestamp(second.getTimestamp());
    }

    if (first.getStimulusList().get(0) >= second.getStimulusList().get(0)) {
      genealogResult.setStimulusList(first.getStimulusList());
    } else {
      genealogResult.setStimulusList(second.getStimulusList());
    }
    genealogResult.setStimulusList(ts);
    return genealogResult;
  }
}
