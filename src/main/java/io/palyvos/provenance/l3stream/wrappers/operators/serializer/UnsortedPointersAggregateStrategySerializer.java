package io.palyvos.provenance.l3stream.wrappers.operators.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.ananke.aggregate.UnsortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogTuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class UnsortedPointersAggregateStrategySerializer extends Serializer<UnsortedPointersAggregateStrategy> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, UnsortedPointersAggregateStrategy object) {
        List<GenealogTuple> ary = new ArrayList<>();
        GenealogTuple earliest = object.getEarliest();
        GenealogTuple current = earliest;
        GenealogTuple latest = object.getLatest();

        if (earliest != null && latest != null) {
            ary.add(current);
            while (current != latest) {
                current = current.getNext();
                ary.add(current);
            }
        } else if (earliest == null && latest == null) {
            // For nonlineage mode
            // do nothing
        }
        kryo.writeObject(output, ary);
    }

    @Override
    public UnsortedPointersAggregateStrategy read(Kryo kryo, Input input, Class<UnsortedPointersAggregateStrategy> type) {
        List<GenealogTuple> ary = kryo.readObject(input, ArrayList.class);
        UnsortedPointersAggregateStrategy spas = new UnsortedPointersAggregateStrategy();

        if (ary.size() == 0) {
            // For nonlineage mode
            spas.setEarliest(null);
            spas.setLatest(null);
        } else if (ary.size() == 1) {
            spas.setEarliest(ary.get(0));
            spas.setLatest(ary.get(0));
        } else {
            GenealogTuple earliest = ary.get(0);
            GenealogTuple current = earliest;
            for (int i = 0; i < ary.size() - 1; i++) {
                current.setNext(ary.get(i + 1));
                current = current.getNext();
            }

            spas.setEarliest(earliest);
            spas.setLatest(ary.get(ary.size() - 1));
        }
        return spas;
    }
}
