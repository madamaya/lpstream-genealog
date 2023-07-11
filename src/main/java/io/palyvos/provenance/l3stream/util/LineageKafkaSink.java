package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializer.LineageSerializer;
import io.palyvos.provenance.l3stream.util.serializer.LineageSerializerLat;
import io.palyvos.provenance.l3stream.util.serializer.LineageSerializerLin;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class LineageKafkaSink {
    public static FlinkKafkaProducer newInstance(String topic, Properties prop, ExperimentSettings settings) {
        if (settings.getLatencyFlag() == 1) {
            return new FlinkKafkaProducer<>(topic, new LineageSerializerLin<>(topic, settings), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        }
        if (settings.getLatencyFlag() == 2) {
            return new FlinkKafkaProducer<>(topic, new LineageSerializerLat<>(topic, settings), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else {
            return new FlinkKafkaProducer<>(topic, new LineageSerializer<>(topic, settings), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        }
    }
}
