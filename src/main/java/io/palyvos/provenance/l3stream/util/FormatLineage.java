package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.util.TimestampedUIDTuple;

import java.util.Set;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class FormatLineage {
    public static String formattedLineage(Set<TimestampedUIDTuple> provenance) {
        StringBuffer sb = new StringBuffer();
        sb.append("(").append(provenance.size()).append(") {\n");
        for (TimestampedUIDTuple t : provenance) {
            sb.append(t).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
