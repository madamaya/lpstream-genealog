package io.palyvos.provenance.l3stream.wrappers.objects;

import io.palyvos.provenance.genealog.GenealogTuple;

public interface L3StreamTuple extends GenealogTuple {

    long getPartitionId();

    void setPartitionId(long partitionId);

    boolean getLineageReliable();

    void setLineageReliable(boolean lineageReliable);

    long getCheckpointId();

    void setCheckpointId(long checkpointId);
}
