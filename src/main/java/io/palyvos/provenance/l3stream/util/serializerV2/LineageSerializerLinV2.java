package io.palyvos.provenance.l3stream.util.serializerV2;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class LineageSerializerLinV2<T> implements KafkaRecordSerializationSchema<L3StreamTupleContainer<T>> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LineageSerializerLinV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<T> tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        Set<TimestampedUIDTuple> lineage = (tuple.getLineageReliable()) ? genealogGraphTraverser.getProvenance(tuple) : new HashSet<>();
        String lineageStr = (tuple.getLineageReliable()) ? FormatLineage.formattedLineage(lineage) : "";

        return new ProducerRecord<>(topic, ("Lineage(" + lineage.size() + ")[" + lineageStr + "]," + tuple.tuple() + "," + tuple.getTimestamp() + "," + tuple.getLineageReliable()).getBytes(StandardCharsets.UTF_8));
    }
}
