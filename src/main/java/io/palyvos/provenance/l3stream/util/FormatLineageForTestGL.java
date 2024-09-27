package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.TimestampedUIDTuple;

import java.util.*;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class FormatLineageForTestGL {
    public static String formattedLineage(Set<TimestampedUIDTuple> provenance) {
        List<TimestampedUIDTuple> list = new ArrayList<>(provenance);
        list.sort(new Comparator<TimestampedUIDTuple>() {
            @Override
            public int compare(TimestampedUIDTuple o1, TimestampedUIDTuple o2) {
                KafkaInputStringGL k1 = (KafkaInputStringGL) o1;
                KafkaInputStringGL k2 = (KafkaInputStringGL) o2;
                if (k1.getStr().compareTo(k2.getStr()) < 0) {
                    return -1;
                } else if (k1.getStr().compareTo(k2.getStr()) == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        });
        StringBuffer sb = new StringBuffer();
        for (Iterator<TimestampedUIDTuple> itr = list.iterator(); itr.hasNext();) {
            TimestampedUIDTuple t = itr.next();
            sb.append(t);
            if (itr.hasNext()) {
                sb.append(",");
            }
        }

        return sb.toString();
    }
}
