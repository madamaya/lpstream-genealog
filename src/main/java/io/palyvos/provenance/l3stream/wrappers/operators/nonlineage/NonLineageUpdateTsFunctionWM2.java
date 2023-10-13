package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageUpdateTsFunctionWM2<T>
    extends RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> implements CheckpointListener {

  private String ip;
  private JedisPool jp;
  private Jedis jedis;
  private transient TimestampAssigner<T> tsAssigner;
  private final WatermarkStrategy<T> watermarkStrategy;
  private long latestTs = -1;
  private int id;

  public NonLineageUpdateTsFunctionWM2(WatermarkStrategy<T> watermarkStrategy, String ip, int id) {
    this.watermarkStrategy = watermarkStrategy;
    this.ip = ip;
    this.id = id;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    tsAssigner = watermarkStrategy.createTimestampAssigner(null);

    jp = new JedisPool(ip, 6379);
    try {
      jedis = jp.getResource();
    } catch (NumberFormatException e) {
        throw new RuntimeException(e);
    }
  }

  @Override
  public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
    L3StreamTupleContainer<T> genealogResult = new L3StreamTupleContainer<>(value);
    genealogResult.copyTimes(value);

    long currentTs = tsAssigner.extractTimestamp(value.tuple(), -1);
    genealogResult.setTimestamp(currentTs);
    latestTs = currentTs;

    return genealogResult;
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    jedis.set(Long.toString(l) + "," + Integer.toString(getRuntimeContext().getIndexOfThisSubtask()) + "," + id, String.valueOf(latestTs));
  }
}
