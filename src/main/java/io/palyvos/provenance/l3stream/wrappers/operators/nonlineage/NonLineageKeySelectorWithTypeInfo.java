package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageKeySelectorWithTypeInfo<IN, KEY> extends
        NonLineageKeySelector<IN, KEY> implements
    ResultTypeQueryable<KEY> {

  private final Class<KEY> clazz;

  public NonLineageKeySelectorWithTypeInfo(
      KeySelector<IN, KEY> delegate, Class<KEY> clazz) {
    super(delegate);
    this.clazz = clazz;
  }

  @Override
  public TypeInformation<KEY> getProducedType() {
    return TypeInformation.of(clazz);
  }
}
