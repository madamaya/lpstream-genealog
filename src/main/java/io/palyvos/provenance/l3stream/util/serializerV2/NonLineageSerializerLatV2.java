package io.palyvos.provenance.l3stream.util.serializerV2;

import io.palyvos.provenance.l3stream.util.FormatStimulusList;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class NonLineageSerializerLatV2<T> implements KafkaRecordSerializationSchema<L3StreamTupleContainer<T>> {
    private String topic;

    public NonLineageSerializerLatV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<T> tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        long ts = System.currentTimeMillis();
        // String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        // return new ProducerRecord<>(topic, latency.getBytes(StandardCharsets.UTF_8));
        tuple.setStimulusList(ts);
        return new ProducerRecord<>(topic, (FormatStimulusList.formatStimulusList(tuple.getStimulusList()) + "," + tuple.getStimulus() + ", OUT:" + tuple.tuple()).getBytes(StandardCharsets.UTF_8));
    }
}
