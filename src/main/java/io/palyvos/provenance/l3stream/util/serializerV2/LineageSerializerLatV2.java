package io.palyvos.provenance.l3stream.util.serializerV2;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedTuple;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class LineageSerializerLatV2<T> implements KafkaRecordSerializationSchema<L3StreamTupleContainer<T>> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LineageSerializerLatV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<T> tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        Set<TimestampedUIDTuple> lineage = null;
        int lineageSize = 0;
        long traverseStart = 0;
        long traverseEnd = 0;
        String lineageStr = "";
        if (tuple.getLineageReliable()) {
            traverseStart = System.nanoTime();
            lineage = genealogGraphTraverser.getProvenance(tuple);
            traverseEnd = System.nanoTime();
            lineageSize = lineage.size();
            lineageStr = FormatLineage.formattedLineage(lineage);
        }
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        String traversalLatency = Long.toString(traverseEnd - traverseStart);

        return new ProducerRecord<>(topic, (latency + "," + tuple.getStimulus() + "," + traversalLatency + ", Lineage(" + lineageSize + ")" + lineageStr + ", OUT:" + tuple.tuple()).getBytes(StandardCharsets.UTF_8));
    }
}
