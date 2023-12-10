package io.palyvos.provenance.util;

import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class BaseTuple implements TimestampedTuple {

  protected long timestamp;
  protected long stimulus;
  protected TimestampsForLatency tfl;
  protected String key;

  public BaseTuple() {}

  public BaseTuple(long timestamp, String key, long stimulus) {
    this.timestamp = timestamp;
    this.stimulus = stimulus;
    this.key = key;
  }

  public BaseTuple(long timestamp, String key) {
    this.timestamp = timestamp;
    this.key = key;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public long getStimulus() {
    return stimulus;
  }

  @Override
  public void setStimulus(long stimulus) {
    this.stimulus = stimulus;
  }

  public TimestampsForLatency getTfl() {
    return tfl;
  }

  public void setTfl(TimestampsForLatency tfl) {
    this.tfl = tfl;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseTuple baseTuple = (BaseTuple) o;
    return timestamp == baseTuple.timestamp
        && stimulus == baseTuple.stimulus
        && Objects.equals(key, baseTuple.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, stimulus, key);
  }

  @Override
  public String toString() {
    return "BaseTuple{"
        + "timestamp="
        + timestamp
        + ", stimulus="
        + stimulus
        + ", key='"
        + key
        + '\''
        + '}';
  }
}
