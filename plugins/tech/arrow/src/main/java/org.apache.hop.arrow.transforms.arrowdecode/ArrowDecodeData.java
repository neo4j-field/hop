package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.arrow.vector.FieldVector;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaArrowVectors;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.lang.ref.WeakReference;
import java.util.Map;

public class ArrowDecodeData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;

  public int inputIndex;

  public ValueMetaArrowVectors arrowValueMeta;

  public Map<String, WeakReference<FieldVector>> vectorMapping = Map.of();
}
