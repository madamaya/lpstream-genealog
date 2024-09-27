package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageChkTsAssigner<T>
    extends RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> implements CheckpointListener {

  private String redisIP = L3conf.REDIS_IP;
  private int redisPort = L3conf.REDIS_PORT;
  private int sourceID;
  private JedisPool jp;
  private Jedis jedis;
  private transient TimestampAssigner<T> tsAssigner;
  private final WatermarkStrategy<T> watermarkStrategy;
  private long latestTs = -1;

  public NonLineageChkTsAssigner(WatermarkStrategy<T> watermarkStrategy, int sourceID) {
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
  public void close() throws Exception {
    super.close();
  }

  @Override
  public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
    L3StreamTupleContainer<T> genealogResult = new L3StreamTupleContainer<>(value.tuple());
    genealogResult.copyTimesWithoutTs(value);
    latestTs = tsAssigner.extractTimestamp(value.tuple(), -1);
    // genealogResult.setTimestamp(currentTs);
    // latestTs = currentTs;

    return genealogResult;
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    jedis.set(Long.toString(l) + "," + Integer.toString(getRuntimeContext().getIndexOfThisSubtask()) + "," + sourceID, String.valueOf(latestTs));
  }
}
