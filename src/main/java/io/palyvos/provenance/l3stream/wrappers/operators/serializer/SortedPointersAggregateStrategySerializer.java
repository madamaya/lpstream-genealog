package io.palyvos.provenance.l3stream.wrappers.operators.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.palyvos.provenance.ananke.aggregate.SortedPointersAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SortedPointersAggregateStrategySerializer extends Serializer<SortedPointersAggregateStrategy> implements Serializable {
    static final long ntime = System.nanoTime();

    @Override
    public void write(Kryo kryo, Output output, SortedPointersAggregateStrategy object) {
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
        // CNFM: earliest == null xor latest == null ケース必要？
        /*
        try {
            fw.write("HOGE");
            fw.write(ary.toString() + "\n");
            fw.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
         */
        kryo.writeObject(output, ary);
    }

    @Override
    public SortedPointersAggregateStrategy read(Kryo kryo, Input input, Class<SortedPointersAggregateStrategy> type) {
        List<GenealogTuple> ary = kryo.readObject(input, ArrayList.class);
        SortedPointersAggregateStrategy spas = new SortedPointersAggregateStrategy();

        if (ary.size() == 0) {
            // For nonlineage mode
            spas.setEarliest(null);
            spas.setLatest(null);
        } else if (ary.size() == 1) {
            // ary.get(0).setStimulus(System.nanoTime());
            spas.setEarliest(ary.get(0));
            spas.setLatest(ary.get(0));
        } else {
            GenealogTuple earliest = ary.get(0);
            GenealogTuple current = earliest;
            for (int i = 0; i < ary.size() - 1; i++) {
                // ary.get(i).setStimulus(System.nanoTime());
                // ary.get(i).setNext(ary.get(i + 1));
                current.setNext(ary.get(i + 1));
                current = current.getNext();
            }
            // ary.get(ary.size() - 1).setStimulus(System.nanoTime());
            // GenealogTuple latest = ary.get(ary.size() - 1);

            spas.setEarliest(earliest);
            spas.setLatest(ary.get(ary.size() - 1));
        }
        return spas;
    }

    public static String formatter(GenealogTuple e, GenealogTuple l) {
        GenealogTuple c = e;
        String ret = "";
        if (e != null && l != null) {
            ret += c;
            while (c != l) {
                if (c.getNext() != null) {
                    c = c.getNext();
                    ret += ("->" + c);
                } else {
                    ret += ("->" + null);
                    break;
                }
            }
        } else if (e == null && l == null) {
            ret += "e == null, l == null";
        } else if (e == null) {
            ret += ("e == null, " + "l = " + l);
        } else if (l == null) {
            ret += ("e = " + e + ", l == null");
        }
        return ret;
    }
}
