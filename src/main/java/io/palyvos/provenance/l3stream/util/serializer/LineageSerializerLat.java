package io.palyvos.provenance.l3stream.util.serializer;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LineageSerializerLat<T> implements KafkaSerializationSchema<L3StreamTupleContainer<T>> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LineageSerializerLat(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<T> tuple, @Nullable Long aLong) {
        String lineage = FormatLineage.formattedLineage(genealogGraphTraverser.getProvenance(tuple));
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, latency.getBytes(StandardCharsets.UTF_8));
    }
}
