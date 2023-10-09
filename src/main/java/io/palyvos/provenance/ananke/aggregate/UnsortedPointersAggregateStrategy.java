package io.palyvos.provenance.ananke.aggregate;

import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import java.util.Iterator;

/**
 * {@link ProvenanceAggregateStrategy} that maintains provenance as a pointer list. The ordering of
 * tuples in the list is arbitrary (depending on tuple arrival order). It does not support
 * out-of-order arrivals of tuples in the window.
 */
/* Modifications copyright (C) 2023 Masaya Yamada */
public class UnsortedPointersAggregateStrategy implements ProvenanceAggregateStrategy {

  GenealogTuple earliest;
  GenealogTuple latest;

  @Override
  public final <T extends GenealogTuple> void addWindowProvenance(T in) {
    if (earliest == null) {
      earliest = in;
      latest = in;
    } else {
      doInsert(in);
    }
  }

  protected <T extends GenealogTuple> void doInsert(T in) {
    latest.setNext(in);
    latest = in;
  }

  @Override
  public final <T extends GenealogTuple> void annotateWindowResult(T result) {
    result.initGenealog(GenealogTupleType.AGGREGATE);
    // The pointer chain goes from the earliest to the latest tuple
    // (U2 (earliest) -> ... -> U1(latest)
    result.setU1(latest);
    result.setU2(earliest);
  }

  @Override
  public final <T extends GenealogTuple> Iterator<GenealogTuple> provenanceIterator(T tuple) {
    return new PointerListIterator(tuple.getU2(), tuple.getU1());
  }

  public GenealogTuple getEarliest() {
    return earliest;
  }

  public void setEarliest(GenealogTuple earliest) {
    this.earliest = earliest;
  }

  public GenealogTuple getLatest() {
    return latest;
  }

  public void setLatest(GenealogTuple latest) {
    this.latest = latest;
  }
}
