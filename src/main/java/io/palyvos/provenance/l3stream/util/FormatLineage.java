package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.util.TimestampedUIDTuple;

import java.util.Set;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class FormatLineage {
    public static String formattedLineage(Set<TimestampedUIDTuple> provenance) {
        StringBuffer sb = new StringBuffer();
        for (TimestampedUIDTuple t : provenance) {
            sb.append(t).append(",");
        }
        if (sb.length() > 0) {
            sb.delete(sb.length() - 1, sb.length());
        } else {
            // throw new IllegalStateException("sb.length() = " + sb.length());
        }
        return sb.toString();
    }
}
