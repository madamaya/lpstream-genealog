package io.palyvos.provenance.usecases.linearroad.noprovenance;


import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;
import java.util.regex.Pattern;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class LinearRoadInputTuple extends BaseTuple {

  private static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
  private int type;
  private long vid;
  private int speed;
  private int xway;
  private int lane;
  private int dir;
  private int seg;
  private int pos;
  private int partitionID;

  public static LinearRoadInputTuple fromReading(String reading) {
    try {
      String[] tokens = DELIMITER_PATTERN.split(reading.trim());
      return new LinearRoadInputTuple(tokens);
    } catch (Exception exception) {
      throw new IllegalArgumentException(String.format(
          "Failed to parse reading: %s", reading), exception);
    }
  }

  public LinearRoadInputTuple(String[] readings) {
    throw new UnsupportedOperationException();
  }

  public LinearRoadInputTuple(int type, long time, int vid, int speed,
      int xway, int lane, int dir, int seg, int pos, long kafkaAppendTime, long stimulus) {
    super(time, String.valueOf(vid), kafkaAppendTime, stimulus);
    this.type = type;
    this.vid = vid;
    this.speed = speed;
    this.xway = xway;
    this.lane = lane;
    this.dir = dir;
    this.seg = seg;
    this.pos = pos;
  }

  public LinearRoadInputTuple(int type, long time, int vid, int speed,
                                 int xway, int lane, int dir, int seg, int pos) {
    super(time, String.valueOf(vid));
    this.type = type;
    this.vid = vid;
    this.speed = speed;
    this.xway = xway;
    this.lane = lane;
    this.dir = dir;
    this.seg = seg;
    this.pos = pos;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public long getVid() {
    return vid;
  }

  public void setVid(long vid) {
    this.vid = vid;
  }

  public int getSpeed() {
    return speed;
  }

  public void setSpeed(int speed) {
    this.speed = speed;
  }

  public int getXway() {
    return xway;
  }

  public void setXway(int xway) {
    this.xway = xway;
  }

  public int getLane() {
    return lane;
  }

  public void setLane(int lane) {
    this.lane = lane;
  }

  public int getDir() {
    return dir;
  }

  public void setDir(int dir) {
    this.dir = dir;
  }

  public int getSeg() {
    return seg;
  }

  public void setSeg(int seg) {
    this.seg = seg;
  }

  public int getPos() {
    return pos;
  }

  public void setPos(int pos) {
    this.pos = pos;
  }

  public int getPartitionID() {
    return partitionID;
  }

  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    LinearRoadInputTuple that = (LinearRoadInputTuple) o;
    return type == that.type &&
        vid == that.vid &&
        speed == that.speed &&
        xway == that.xway &&
        lane == that.lane &&
        dir == that.dir &&
        seg == that.seg &&
        pos == that.pos;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), type, vid, speed, xway, lane, dir, seg, pos);
  }

  @Override
  public String toString() {
    return type + "," + getTimestamp() + "," + vid + "," + speed + ","
        + xway + "," + lane + "," + dir + "," + seg + "," + pos + "," + getKey();
  }
}
