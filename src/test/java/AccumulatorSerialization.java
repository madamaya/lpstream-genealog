import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.usecases.linearroad.noprovenance.VehicleTuple;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class AccumulatorSerialization {
    final static int N = 3;
    final static boolean DEBUG = true;
    public static void main(String[] args) throws Exception {
        // Create VehicleTuple set
        Set<VehicleTuple> vset = new HashSet<>();
        for (int i = 0; i < N; i++) {
            vset.add(vehicleTupleGen());
            Thread.sleep(10);
        }

        if (DEBUG) {
            displayCollection(vset, "vset");
        }

        // Create L3StreamTupleContainer<VehicleTuple> from vset
        Set<L3StreamTupleContainer<VehicleTuple>> l3set = new HashSet<>();
        for (VehicleTuple tuple : vset) {
            L3StreamTupleContainer<VehicleTuple> tmp = new L3StreamTupleContainer<>(tuple);
            tmp.initGenealog(GenealogTupleType.SOURCE);
            tmp.setTimestamp(tuple.getTimestamp());
            tmp.setStimulus(tuple.getStimulus());
            l3set.add(tmp);
        }

        if (DEBUG) {
            displayCollection(l3set, "l3set");
        }

        // Create AggregateStrategy
        SortedPointersAggregateStrategy strategy = new SortedPointersAggregateStrategy();

        // Ingest l3set tuples into the accumulator
        for (L3StreamTupleContainer<VehicleTuple> tuple : l3set) {
            strategy.addWindowProvenance(tuple);
        }

        if (DEBUG) {
            System.out.println(strategy);
        }
    }

    private static VehicleTuple vehicleTupleGen() {
        return new VehicleTuple(
                System.currentTimeMillis(),
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                true,
                System.currentTimeMillis()
        );
    }

    private static <T> void displayCollection(Collection<T> collection, String flag) {
        System.out.println("Display: " + flag + " {");
        for (T element : collection) {
            System.out.println(">> " + element + ",");
        }
        System.out.println("}");
    }

    private static <T> void displayCollection(Collection<T> collection) {
        displayCollection(collection, "");
    }
}
