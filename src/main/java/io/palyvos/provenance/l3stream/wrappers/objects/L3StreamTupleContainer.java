package io.palyvos.provenance.l3stream.wrappers.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.java.tuple.Tuple2;

public class L3StreamTupleContainer<T> implements L3StreamTuple {

    private GenealogData genealogData;
    private long partitionId;
    private long latencyTs;
    private TimestampsForLatency tfl;
    private long timestamp;
    private boolean lineageReliable;
    private long checkpointId;
    private final T tuple;

    public L3StreamTupleContainer(T tuple) {
        this.tuple = tuple;
        this.lineageReliable = true;
    }

    public L3StreamTupleContainer(L3StreamTupleContainer<T> tuple) {
        this(tuple.tuple());
        this.genealogData = tuple.getGenealogData();
        this.partitionId = tuple.getPartitionId();
        this.latencyTs = tuple.getStimulus();
        this.timestamp = tuple.getTimestamp();
        this.lineageReliable = tuple.getLineageReliable();
        this.checkpointId = tuple.getCheckpointId();
    }

    public T tuple() {
        return this.tuple;
    }

    public void copyTimes(L3StreamTupleContainer value) {
        this.timestamp = value.getTimestamp();
        this.latencyTs = value.getStimulus();
    }

    public void copyTimesTFL(L3StreamTupleContainer value) {
        this.timestamp = value.getTimestamp();
        this.tfl = value.getTfl();
    }

    public void copyTimes(L3StreamTupleContainer value1, L3StreamTupleContainer value2) {
        this.timestamp = Math.max(value1.getTimestamp(), value2.getTimestamp());
        this.latencyTs = Math.max(value1.getStimulus(), value2.getStimulus());
    }

    public void copyTimesTFL(L3StreamTupleContainer value1, L3StreamTupleContainer value2) {
        this.timestamp = Math.max(value1.getTimestamp(), value2.getTimestamp());
        if (value1.getTfl().ts1 > value2.getTfl().ts1) {
            this.tfl = value1.getTfl();
            if (value1.getTfl().ts2 < value2.getTfl().ts2) {
                TimestampsForLatency tmp = value2.getTfl();
                this.tfl.setTs2(tmp.ts2);
                this.tfl.setTs3(tmp.ts3);
                this.tfl.setTs4(tmp.ts4);
                this.tfl.setTs5(tmp.ts5);
                this.tfl.setTs6(tmp.ts6);
                this.tfl.setTs7(tmp.ts7);
                this.tfl.setTs8(tmp.ts8);
                this.tfl.setTs9(tmp.ts9);
                this.tfl.setTs10(tmp.ts10);
            }
        } else {
            this.tfl = value2.getTfl();
            if (value1.getTfl().ts2 > value2.getTfl().ts2) {
                TimestampsForLatency tmp = value1.getTfl();
                this.tfl.setTs2(tmp.ts2);
                this.tfl.setTs3(tmp.ts3);
                this.tfl.setTs4(tmp.ts4);
                this.tfl.setTs5(tmp.ts5);
                this.tfl.setTs6(tmp.ts6);
                this.tfl.setTs7(tmp.ts7);
                this.tfl.setTs8(tmp.ts8);
                this.tfl.setTs9(tmp.ts9);
                this.tfl.setTs10(tmp.ts10);
            }
        }

    }

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
        genealogData = new GenealogData();
        genealogData.init(tupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return genealogData;
    }

    @Override
    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public boolean getLineageReliable() {
        return lineageReliable;
    }

    @Override
    public void setLineageReliable(boolean lineageReliable) {
        this.lineageReliable = lineageReliable;
    }

    @Override
    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
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
        return latencyTs;
    }

    @Override
    public void setStimulus(long stimulus) {
        this.latencyTs = stimulus;
    }


    @Override
    public TimestampsForLatency getTfl() {
        return tfl;
    }

    @Override
    public void setTfl(TimestampsForLatency tfl) {
        this.tfl = tfl;
    }

    @Override
    public String toString() {
        return tuple.toString();
    }
}
