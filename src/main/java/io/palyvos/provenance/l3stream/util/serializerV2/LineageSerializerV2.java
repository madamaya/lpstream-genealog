package io.palyvos.provenance.l3stream.util.serializerV2;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LineageSerializerV2<T> implements KafkaRecordSerializationSchema<L3StreamTupleContainer<T>> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LineageSerializerV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<T> tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String lineage = "";
        if (tuple.getLineageReliable()) {
            lineage = FormatLineage.formattedLineage(genealogGraphTraverser.getProvenance(tuple));
        }
        String ret = "{\"OUT\":\"" + tuple.tuple() + "\"}";
        return new ProducerRecord<>(topic, ret.getBytes(StandardCharsets.UTF_8));
    }
}
