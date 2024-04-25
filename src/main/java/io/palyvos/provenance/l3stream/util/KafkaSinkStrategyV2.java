package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Properties;

public interface KafkaSinkStrategyV2 {
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings);
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings, Properties props);
}
