package io.palyvos.provenance.l3stream.util;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.LineageModeStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.NonLineageModeStrategy;
import io.palyvos.provenance.util.ExperimentSettings;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Supplier;

public class L3Settings extends ExperimentSettings implements Serializable {

    @Parameter(names = "--lineageMode", required = true, converter = L3OpWrapperStrategyConverter.class)
    transient private Function<Supplier<ProvenanceAggregateStrategy>, L3OpWrapperStrategy> l3OpWrapperStrategy;

    public Function<Supplier<ProvenanceAggregateStrategy>, L3OpWrapperStrategy> l3OpWrapperStrategy() {
        return l3OpWrapperStrategy;
    }

    public static L3Settings newInstance(String[] args) {
        L3Settings settings = new L3Settings();
        JCommander.newBuilder().addObject(settings).build().parse(args);
        return settings;
    }

    public String getLineageMode() {
        if (l3OpWrapperStrategy.apply(null).getClass() == NonLineageModeStrategy.class) {
            return "NonLineageMode";
        } else {
            return "LineageMode";
        }
    }

    private static class L3OpWrapperStrategyConverter
            implements IStringConverter<Function<Supplier<ProvenanceAggregateStrategy>, L3OpWrapperStrategy>> {

        @Override
        public Function<Supplier<ProvenanceAggregateStrategy>, L3OpWrapperStrategy> convert(
                String value) {
            assert value == "Lineage" || value == "nonLineage";
            switch (value) {
                case "Lineage":
                    return (Function<Supplier<ProvenanceAggregateStrategy>, L3OpWrapperStrategy>)
                            LineageModeStrategy::new;
                case "nonLineage":
                    return (Function<Supplier<ProvenanceAggregateStrategy>, L3OpWrapperStrategy>)
                            NonLineageModeStrategy::new;
                default:
                    throw new IllegalArgumentException("Undefined lineage mode is provided.");
            }
        }
    }
}
