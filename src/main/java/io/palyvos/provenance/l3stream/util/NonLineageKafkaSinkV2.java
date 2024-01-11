package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializer.NonLineageSerializer;
import io.palyvos.provenance.l3stream.util.serializer.NonLineageSerializerLat;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerLatRawV2;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerLatV2;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class NonLineageKafkaSinkV2 implements KafkaSinkStrategyV2 {
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
                    .setRecordSerializer(new NonLineageSerializerLatRawV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        if (settings.getLatencyFlag() == 0) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new NonLineageSerializerLatV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
            //return new FlinkKafkaProducer<>(topic, new NonLineageSerializerLatV2<>(topic), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else if (settings.getLatencyFlag() == 1) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new NonLineageSerializerV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
            // return new FlinkKafkaProducer<>(topic, new NonLineageSerializerV2<>(topic), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else {
            throw new IllegalArgumentException("NonLineageKafkaSink");
        }
    }
}
