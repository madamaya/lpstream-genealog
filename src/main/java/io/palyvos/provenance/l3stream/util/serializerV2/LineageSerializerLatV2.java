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
        String lineageStr = "";
        Set lineage = null;
        int lineageSize = 0;
        if (tuple.getLineageReliable()) {
            lineage = genealogGraphTraverser.getProvenance(tuple);
            lineageSize = lineage.size();
            lineageStr = FormatLineage.formattedLineage(lineage);
        }
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());

        return new ProducerRecord<>(topic, (latency + "," + tuple.getStimulus() + ", Lineage(" + lineageSize + ")" + lineageStr + ", OUT:" + tuple.tuple()).getBytes(StandardCharsets.UTF_8));
    }
}
