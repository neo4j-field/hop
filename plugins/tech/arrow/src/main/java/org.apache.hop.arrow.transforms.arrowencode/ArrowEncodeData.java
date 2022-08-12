package org.apache.hop.arrow.transforms.arrowencode;

import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class ArrowEncodeData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;

  public List<Integer> sourceFieldIndexes;

  public Schema arrowSchema;

  /** Current batch size. */
  public int count = 0;

  /** Number of batches processed. */
  public int batches = 0;

  public FieldVector[] vectors = new FieldVector[] {};

  public BaseWriter[] writers = new BaseWriter[] {};
}
