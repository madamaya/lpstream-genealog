package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageKeySelectorWithTypeInfo<IN, KEY> extends
        LineageKeySelector<IN, KEY> implements
    ResultTypeQueryable<KEY> {

  private final Class<KEY> clazz;

  public LineageKeySelectorWithTypeInfo(
      KeySelector<IN, KEY> delegate, Class<KEY> clazz) {
    super(delegate);
    this.clazz = clazz;
  }

  @Override
  public TypeInformation<KEY> getProducedType() {
    return TypeInformation.of(clazz);
  }
}
