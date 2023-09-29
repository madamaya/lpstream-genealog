package io.palyvos.provenance.l3stream.util.serializer;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class NonLineageSerializer<T> implements KafkaSerializationSchema<L3StreamTupleContainer<T>> {
    private String topic;

    public NonLineageSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<T> tuple, @Nullable Long aLong) {
        String ret = "{\"OUT\":\"" + tuple.tuple() + "\",\"CPID\":\"" + tuple.getCheckpointId() + "\"}";
        return new ProducerRecord<>(topic, ret.getBytes(StandardCharsets.UTF_8));
    }
}
