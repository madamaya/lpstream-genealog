package io.palyvos.provenance.util;

import java.util.List;
import java.util.Objects;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class BaseTuple implements TimestampedTuple {

  protected long timestamp;
  protected long stimulus;
  protected String key;
  private List<Long> stimulusList;

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

  public List<Long> getStimulusList() {
    return stimulusList;
  }

  public void setStimulusList(List<Long> stimulusList) {
    this.stimulusList = stimulusList;
  }

  public void setStimulusList(long stimulus) {
    this.stimulusList.add(stimulus);
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
