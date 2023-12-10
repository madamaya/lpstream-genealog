package io.palyvos.provenance.usecases;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.java.tuple.Tuple2;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class CountTupleGL extends CountTuple implements GenealogTuple {

  private GenealogData gdata;

  public CountTupleGL(long timestamp, String key, long count) {
    super(timestamp, key, count);
  }

  public CountTupleGL(long timestamp, String key, long stimulus, long count) {
    super(timestamp, key, stimulus, count);
  }

  @Override
  public GenealogTuple getU1() {
    return gdata.getU1();
  }

  @Override
  public void setU1(GenealogTuple u1) {
    gdata.setU1(u1);
  }

  @Override
  public GenealogTuple getU2() {
    return gdata.getU2();
  }

  @Override
  public void setU2(GenealogTuple u2) {
    gdata.setU2(u2);
  }

  @Override
  public GenealogTuple getNext() {
    return gdata.getNext();
  }

  @Override
  public void setNext(GenealogTuple next) {
    gdata.setNext(next);
  }

  @Override
  public GenealogTupleType getTupleType() {
    return gdata.getTupleType();
  }

  @Override
  public void initGenealog(GenealogTupleType tupleType) {
    gdata = new GenealogData();
    gdata.init(tupleType);
  }

  @Override
  public long getUID() {
    return gdata.getUID();
  }

  @Override
  public void setUID(long uid) {
    gdata.setUID(uid);
  }

  @Override
  public GenealogData getGenealogData() {
    return gdata;
  }

  @Override
  public TimestampsForLatency getTfl() {
    return super.getTfl();
  }

  @Override
  public void setTfl(TimestampsForLatency tfl) {
    super.setTfl(tfl);
  }
}
