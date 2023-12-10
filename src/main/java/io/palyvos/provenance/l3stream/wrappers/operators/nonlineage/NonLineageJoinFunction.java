package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.JoinFunction;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageJoinFunction<IN1, IN2, OUT>
    implements JoinFunction<
        L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> {

  private final JoinFunction<IN1, IN2, OUT> delegate;

  public NonLineageJoinFunction(JoinFunction<IN1, IN2, OUT> delegate) {
    this.delegate = delegate;
  }

  @Override
  public L3StreamTupleContainer<OUT> join(
      L3StreamTupleContainer<IN1> first, L3StreamTupleContainer<IN2> second) throws Exception {
    OUT result = delegate.join(first.tuple(), second.tuple());
    L3StreamTupleContainer<OUT> genealogResult = new L3StreamTupleContainer<>(result);
    // GenealogJoinHelper.INSTANCE.annotateResult(first, second, genealogResult);
    // genealogResult.copyTimes(first, second);
    genealogResult.copyTimesTFL(first, second);
    return genealogResult;
  }
}
