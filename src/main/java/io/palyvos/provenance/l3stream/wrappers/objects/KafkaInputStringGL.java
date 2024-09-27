package io.palyvos.provenance.l3stream.wrappers.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class KafkaInputStringGL extends KafkaInputString implements GenealogTuple {
    private GenealogData gdata;

    public KafkaInputStringGL(int partitionID, String str, long kafkaAppandTime, long stimulus) {
        super(partitionID, str, kafkaAppandTime, stimulus);
    }

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
        this.gdata = new GenealogData();
        this.gdata.init(tupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return gdata;
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestamp(long timestamp) {
        throw new UnsupportedOperationException();
    }
}
