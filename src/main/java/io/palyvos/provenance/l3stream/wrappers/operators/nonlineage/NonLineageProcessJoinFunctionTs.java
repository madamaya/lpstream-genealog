package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageProcessJoinFunctionTs<IN1, IN2, OUT> extends
    ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> {

  private final ProcessJoinFunction<IN1, IN2, OUT> delegate;

  public NonLineageProcessJoinFunctionTs(
      ProcessJoinFunction<IN1, IN2, OUT> delegate) {
    Validate.notNull(delegate, "delegate");
    this.delegate = delegate;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    delegate.open(parameters);
  }

  @Override
  public void processElement(L3StreamTupleContainer<IN1> left,
      L3StreamTupleContainer<IN2> right,
      ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>>.Context ctx,
      Collector<L3StreamTupleContainer<OUT>> out) throws Exception {
    delegate.processElement(left.tuple(), right.tuple(),
        (ProcessJoinFunction<IN1, IN2, OUT>.Context) ctx, new CollectorAdapter<>(left, right, out));
  }


  private static class CollectorAdapter<T1, T2, O> implements Collector<O> {

    private final L3StreamTupleContainer<T1> left;
    private final L3StreamTupleContainer<T2> right;
    private final Collector<L3StreamTupleContainer<O>> delegate;

    public CollectorAdapter(L3StreamTupleContainer<T1> left, L3StreamTupleContainer<T2> right,
        Collector<L3StreamTupleContainer<O>> delegate) {
      this.left = left;
      this.right = right;
      this.delegate = delegate;
    }

    @Override
    public void collect(O record) {
      L3StreamTupleContainer<O> genealogResult = new L3StreamTupleContainer<>(record);
      // GenealogJoinHelper.INSTANCE.annotateResult(left, right, genealogResult);
      genealogResult.copyTimesWithoutTs(left, right);
      genealogResult.setDominantOpTime(System.nanoTime() - genealogResult.getDominantOpTime());
      delegate.collect(genealogResult);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

}
