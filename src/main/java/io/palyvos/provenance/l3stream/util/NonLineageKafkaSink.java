package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializer.NonLineageSerializer;
import io.palyvos.provenance.l3stream.util.serializer.NonLineageSerializerLat;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class NonLineageKafkaSink {
    public static FlinkKafkaProducer newInstance(String topic, Properties prop, ExperimentSettings settings) {
        if (settings.getLatencyFlag() == 1) {
            return new FlinkKafkaProducer<>(topic, new NonLineageSerializerLat<>(topic), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else {
            return new FlinkKafkaProducer<>(topic, new NonLineageSerializer<>(topic), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        }
    }
}
