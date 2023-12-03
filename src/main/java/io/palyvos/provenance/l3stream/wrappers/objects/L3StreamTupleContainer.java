package io.palyvos.provenance.l3stream.wrappers.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTupleType;

import java.util.List;

public class L3StreamTupleContainer<T> implements L3StreamTuple {

    private GenealogData genealogData;
    private long partitionId;
    private long latencyTs;
    private long timestamp;
    private boolean lineageReliable;
    private long checkpointId;
    private final T tuple;
    private List<Long> stimulusList;

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

    public void copyTimes(L3StreamTupleContainer value1, L3StreamTupleContainer value2) {
        this.timestamp = Math.max(value1.getTimestamp(), value2.getTimestamp());
        this.latencyTs = Math.max(value1.getStimulus(), value2.getStimulus());
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
    public String toString() {
        return tuple.toString();
    }

    @Override
    public List<Long> getStimulusList() {
        return this.stimulusList;
    }

    @Override
    public void setStimulusList(List<Long> stimulusList) {
        this.stimulusList = stimulusList;
    }

    @Override
    public void setStimulusList(long stimulus) {
        this.stimulusList.add(stimulus);
    }
}
