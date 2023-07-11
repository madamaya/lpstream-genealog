package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.util.L3Settings;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class NonLineageKafkaSerializer<IN> implements KafkaSerializationSchema<L3StreamTupleContainer<IN>> {
    private final KafkaSerializationSchema<IN> delegate;

    public NonLineageKafkaSerializer(KafkaSerializationSchema<IN> delegate, L3Settings settings) {
        this.delegate = delegate;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<IN> t, @Nullable Long aLong) {
        return delegate.serialize(t.tuple(), aLong);
    }
}
