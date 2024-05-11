package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.util.TimestampedUIDTuple;

import java.util.Iterator;
import java.util.Set;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class FormatLineage {
    public static String formattedLineage(Set<TimestampedUIDTuple> provenance) {
        StringBuffer sb = new StringBuffer();
        for (Iterator<TimestampedUIDTuple> itr = provenance.iterator(); itr.hasNext();) {
            TimestampedUIDTuple t = itr.next();
            sb.append(t);
            if (itr.hasNext()) {
                sb.append(",");
            }
        }

        return sb.toString();
    }
}
