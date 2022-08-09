package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaArrowVectors;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Arrays;
import java.util.List;

public class ArrowDecode extends BaseTransform<ArrowDecodeMeta, ArrowDecodeData> {
  /**
   * Encode Arrow RecordBatch into Hop Rows.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta          the meta object
   * @param data          the data object to store temporary data, database connections, caches, result sets,
   *                      hashtables etc.
   * @param copyNr        The copynumber for this transform.
   * @param pipelineMeta  The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline      The (running) pipeline to obtain information shared among the transforms.
   */
  public ArrowDecode(
    TransformMeta transformMeta,
    ArrowDecodeMeta meta,
    ArrowDecodeData data,
    int copyNr,
    PipelineMeta pipelineMeta,
    Pipeline pipeline
  ) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }


  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    // Setup a schema?
    //
    if (first) {
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      String sourceFieldName = resolve(meta.getSourceFieldName());
      data.inputIndex = getInputRowMeta().indexOfValue(sourceFieldName);
      if (data.inputIndex < 0) {
        throw new HopException("Unable to find Arrow source field: " + sourceFieldName);
      }
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.inputIndex);
      if (!(valueMeta instanceof ValueMetaArrowVectors)) {
        throw new HopException(
                "We can only decode Arrow data types and field "
                        + sourceFieldName
                        + " is of type "
                        + valueMeta.getTypeDesc());
      }
      data.arrowValueMeta = (ValueMetaArrowVectors) valueMeta;
      data.vectorIndices = new int[meta.getTargetFields().size()];
    }

    FieldVector[] vectors = (FieldVector[]) row[data.inputIndex];
    if (vectors == null || vectors.length == 0) {
      throw new HopException("No vectors provided");
    }
    int rowCount = vectors[0].getValueCount();
    if (rowCount == 0) {
      // XXX bail out?
      return false;
    }

    // Build a mapping between the incoming vectors and the outgoing fields if this
    // is our first batch.
    //
    /*
    if (first) {
      first = false;
      for (int j = 0; j < data.vectorIndices.length; j++) {
        int index = -1;

        for (int n = 0; n < vectors.length; n++) {
          String vectorName = vectors[n].getName();
          String srcFieldName = resolve(meta.get)
          if (name.equals(meta.getTargetFields().get(j).getSourceField())) {
            index = n;
            break;
          }
        }
        data.vectorIndices[j] = index;
      }
    }*/

    //
    for (int i = 0; i < rowCount; i++) {
      Object[] outputRow = convertToRow(i, row, vectors, data.vectorIndices);
      putRow(data.outputRowMeta, outputRow);
    }

    // Release vectors
    //
    for (FieldVector vector : vectors) {
      vector.close();
    }

    logBasic("decoded " + rowCount + " rows from Arrow vectors");

    return true;
  }

  /**
   * Generate a Hop row from a row-based slice across the vectors.
   *
   * @param rowNum row index in the Arrow Vectors
   * @param inputRow incoming Hop row
   * @param vectors array of Arrow Vectors
   * @param indices int array mapping of...
   * @return new Hop row of Objects
   */
  private Object[] convertToRow(int rowNum, Object[] inputRow, FieldVector[] vectors, int[] indices) throws HopException {
    Object[] outputRow = RowDataUtil.createResizedCopy(inputRow, data.outputRowMeta.size());

    // ...and append new fields.
    //
    int rowIndex = getInputRowMeta().size();

    for (TargetField targetField : meta.getTargetFields()) {
      String srcFieldName = resolve(targetField.getSourceField());

      // XXX This section is redundantly called.
      FieldVector vector = Arrays.stream(vectors)
              .filter(v -> v.getName().equalsIgnoreCase(srcFieldName))
              .findFirst()
              .get(); // XXX yolo
      Object hopValue = vector.getObject(rowNum);
      IValueMeta standardValueMeta =
              ValueMetaFactory.createValueMeta("standard", getStandardHopType(vector.getField()));
      IValueMeta targetValueMeta = targetField.createTargetValueMeta(this);
      standardValueMeta.setConversionMask(targetValueMeta.getConversionMask());

      outputRow[rowIndex++] = targetValueMeta.convertData(standardValueMeta, hopValue);
    }

    // We overwrite the original Arrow object...
    //
    outputRow[data.inputIndex] = new FieldVector[0]; // XXX use null?

    return outputRow;
  }

  public static int getStandardHopType(Field field) throws HopException {
    ArrowType.ArrowTypeID typeId = field.getFieldType().getType().getTypeID();
    switch (typeId) {
      case Int:
        return IValueMeta.TYPE_INTEGER;
      case Utf8:
      case LargeUtf8:
        return IValueMeta.TYPE_STRING;
      case FloatingPoint:
        return IValueMeta.TYPE_NUMBER;
      case Bool:
        return IValueMeta.TYPE_BOOLEAN;
      default:
        throw new HopException("Arrow type " + typeId + " is not handled yet.");
    }
  }
}
