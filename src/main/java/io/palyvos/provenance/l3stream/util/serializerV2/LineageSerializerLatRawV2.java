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
import java.util.Set;

public class LineageSerializerLatRawV2<T> implements KafkaRecordSerializationSchema<L3StreamTupleContainer<T>> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LineageSerializerLatRawV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<T> tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        long traversalStartTime = System.nanoTime();
        Set<TimestampedUIDTuple> lineage = (tuple.getLineageReliable()) ? genealogGraphTraverser.getProvenance(tuple) : null;
        String lineageStr = (tuple.getLineageReliable()) ? FormatLineage.formattedLineage(lineage) : "";
        long traversalEndTime = System.nanoTime();

        String traversalTime = Long.toString(traversalEndTime - traversalStartTime);
        return new ProducerRecord<>(topic, (tuple.getStimulus() + "," + tuple.getKafkaAppendTime() + "," + traversalTime + ", Lineage(" + lineage.size() + ")" + lineageStr + ", OUT:" + tuple.tuple()).getBytes(StandardCharsets.UTF_8));
    }
}
