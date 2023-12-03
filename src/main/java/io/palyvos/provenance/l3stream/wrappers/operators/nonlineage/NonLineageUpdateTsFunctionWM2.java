package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
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

  private String redisIP = L3conf.REDIS_IP;
  private int redisPort = L3conf.REDIS_PORT;
  private int sourceID;
  private JedisPool jp;
  private Jedis jedis;
  private transient TimestampAssigner<T> tsAssigner;
  private final WatermarkStrategy<T> watermarkStrategy;
  private long latestTs = -1;

  public NonLineageUpdateTsFunctionWM2(WatermarkStrategy<T> watermarkStrategy, int sourceID) {
    this.watermarkStrategy = watermarkStrategy;
    this.sourceID = sourceID;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    tsAssigner = watermarkStrategy.createTimestampAssigner(null);

    jp = new JedisPool(redisIP, redisPort);
    try {
      jedis = jp.getResource();
    } catch (NumberFormatException e) {
        throw new RuntimeException(e);
    }
  }

  @Override
  public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
    long ts = System.currentTimeMillis();
    L3StreamTupleContainer<T> genealogResult = new L3StreamTupleContainer<>(value);
    // genealogResult.copyTimes(value);
    genealogResult.setTimestamp(value.getTimestamp());
    genealogResult.setStimulusList(value.getStimulusList());
    genealogResult.setStimulusList(ts);

    long currentTs = tsAssigner.extractTimestamp(value.tuple(), -1);
    genealogResult.setTimestamp(currentTs);
    latestTs = currentTs;

    return genealogResult;
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    jedis.set(Long.toString(l) + "," + Integer.toString(getRuntimeContext().getIndexOfThisSubtask()) + "," + sourceID, String.valueOf(latestTs));
  }
}
