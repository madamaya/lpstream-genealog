package io.palyvos.provenance.l3stream.wrappers.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTupleType;

/* This implementation is based on ProvenanceTupleContainer (io.palyvos.provenance.ananke.functions) */
public class L3StreamTupleContainer<T> implements L3StreamTuple {

    private GenealogData genealogData;
    private long partitionId;
    private long timestamp;
    private boolean lineageReliable;
    private long checkpointId;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long latencyTs = Long.MAX_VALUE;
    private final T tuple;

    public L3StreamTupleContainer(T tuple) {
        this.tuple = tuple;
        this.lineageReliable = true;
    }

    public L3StreamTupleContainer(L3StreamTupleContainer<T> tuple) {
        this(tuple.tuple());
        this.genealogData = tuple.getGenealogData();
        this.partitionId = tuple.getPartitionId();
        this.timestamp = tuple.getTimestamp();
        this.lineageReliable = tuple.getLineageReliable();
        this.checkpointId = tuple.getCheckpointId();
        this.dominantOpTime = tuple.getDominantOpTime();
        this.kafkaAppendTime = tuple.getKafkaAppendTime();
        this.latencyTs = tuple.getStimulus();
    }

    public T tuple() {
        return this.tuple;
    }

    public void copyTimes(L3StreamTupleContainer value) {
        this.timestamp = value.getTimestamp();
        this.dominantOpTime = value.getDominantOpTime();
        this.kafkaAppendTime = value.getKafkaAppendTime();
        this.latencyTs = value.getStimulus();
    }

    public void copyTimes(L3StreamTupleContainer value1, L3StreamTupleContainer value2) {
        this.timestamp = Math.max(value1.getTimestamp(), value2.getTimestamp());
        this.dominantOpTime = Math.max(value1.getDominantOpTime(), value2.getDominantOpTime());
        this.kafkaAppendTime = Math.max(value1.getKafkaAppendTime(), value2.getKafkaAppendTime());
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

    public long getDominantOpTime() {
        return dominantOpTime;
    }

    public void setDominantOpTime(long dominantOpTime) {
        this.dominantOpTime = dominantOpTime;
    }

    public long getKafkaAppendTime() {
        return kafkaAppendTime;
    }

    public void setKafkaAppendTime(long kafkaAppendTime) {
        this.kafkaAppendTime = kafkaAppendTime;
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
}
