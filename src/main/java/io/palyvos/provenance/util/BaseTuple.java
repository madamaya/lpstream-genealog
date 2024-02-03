package io.palyvos.provenance.util;

import java.util.Objects;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class BaseTuple implements TimestampedTuple {

  protected long timestamp;
  protected long stimulus;
  protected long kafkaAppendTime;
  protected String key;

  public BaseTuple() {}

  public BaseTuple(long timestamp, String key, long kafkaAppendTime, long stimulus) {
    this.timestamp = timestamp;
    this.key = key;
    this.kafkaAppendTime = kafkaAppendTime;
    this.stimulus = stimulus;
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

  public long getKafkaAppendTime() {
    return kafkaAppendTime;
  }

  public void setKafkaAppendTime(long kafkaAppendTime) {
    this.kafkaAppendTime = kafkaAppendTime;
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
