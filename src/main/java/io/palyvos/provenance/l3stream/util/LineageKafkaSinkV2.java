package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializer.LineageSerializer;
import io.palyvos.provenance.l3stream.util.serializer.LineageSerializerLat;
import io.palyvos.provenance.l3stream.util.serializer.LineageSerializerLin;
import io.palyvos.provenance.l3stream.util.serializerV2.*;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class LineageKafkaSinkV2 implements KafkaSinkStrategyV2 {
    @Override
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings) {
        return newInstance(topic, broker, settings, new Properties(), false);
    }

    @Override
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings, Boolean rawStimulusFlag) {
        return newInstance(topic, broker, settings, new Properties(), rawStimulusFlag);
    }

    @Override
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings, Properties props) {
        return newInstance(topic, broker, settings, props, false);
    }

    @Override
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings, Properties props, Boolean rawStimulusFlag) {
        if (rawStimulusFlag) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerLatRawV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        if (settings.getLatencyFlag() == 0) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerLatV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 1) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 2) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerLinV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            throw new IllegalArgumentException("LineageKafkaSink");
        }
    }
}
