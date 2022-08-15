package org.apache.hop.pipeline.transforms.loadsave.validator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.hop.core.row.value.ValueMetaFactory;

public class MetaTypeIdLoadSaveValidator implements IFieldLoadSaveValidator<Integer>{

  private List<Integer> typeIdSet;
  private Random random = new Random();

  /**
   * A variation of an IntLoadSaveValidator specifically for testing valid ValueMeta type ids.
   */
  public MetaTypeIdLoadSaveValidator() {
    typeIdSet = Arrays
        .stream(ValueMetaFactory.getAllValueMetaNames())
        .map(ValueMetaFactory::getIdForValueMeta)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Integer getTestObject() {
    return typeIdSet.get(random.nextInt(typeIdSet.size()));
  }

  @Override
  public boolean validateTestObject(Integer testObject, Object actual) {
    return testObject.equals(actual);
  }
}
