package org.apache.hop.arrow.transforms.arrowdecode;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaArrowVectors;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class ArrowDecode extends BaseTransform<ArrowDecodeMeta, ArrowDecodeData> {
  /**
   * Encode Arrow RecordBatch into Hop Rows.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta the meta object
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hashtables etc.
   * @param copyNr The copynumber for this transform.
   * @param pipelineMeta The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transforms.
   */
  public ArrowDecode(
      TransformMeta transformMeta,
      ArrowDecodeMeta meta,
      ArrowDecodeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
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
      data.vectorMapping = new HashMap<>();
    }

    // Get a reference to our vectors. Bail if we have none or garbage.
    //
    FieldVector[] vectors = (FieldVector[]) row[data.inputIndex];
    if (vectors == null || vectors.length == 0) {
      throw new HopException("No vectors provided.");
    }
    int rowCount = vectors[0].getValueCount();
    if (rowCount == 0) {
      // XXX bail out?
      logError("Empty vector: " + vectors[0]);
      return false;
    }

    // Build a mapping between the incoming vectors and the outgoing fields if this
    // is our first batch.
    //
    if (first) {
      first = false;
      for (TargetField targetField : meta.getTargetFields()) {
        String fieldName = resolve(targetField.getSourceField());
        for (int i = 0; i < vectors.length; i++) {
          if (vectors[i].getName().equals(fieldName)) {
            data.vectorMapping.put(fieldName, i);
            break;
          }
        }
      }
    }

    // Decode!
    //
    for (int rowNum = 0; rowNum < rowCount; rowNum++) {
      Object[] outputRow = convertToRow(vectors, rowNum, row);
      putRow(data.outputRowMeta, outputRow);
    }

    // Release vectors
    //
    for (FieldVector vector : vectors) {
      vector.close();
    }

    logDetailed("decoded " + rowCount + " rows from Arrow vectors");

    return true;
  }

  /**
   * Generate a Hop row from a row-based slice across the vectors.
   *
   * @param rowNum row index in the Arrow Vectors
   * @param inputRow incoming Hop row
   * @return new Hop row of Objects
   */
  private Object[] convertToRow(FieldVector[] vectors, int rowNum, Object[] inputRow)
      throws HopException {
    Object[] outputRow = RowDataUtil.createResizedCopy(inputRow, data.outputRowMeta.size());

    // ...and append new fields.
    //
    int endOfRow = getInputRowMeta().size() + 1;

    for (TargetField targetField : meta.getTargetFields()) {
      String srcFieldName = resolve(targetField.getSourceField());
      String outFieldName = resolve(targetField.getTargetFieldName());

      int vectorIndex = data.vectorMapping.getOrDefault(srcFieldName, -1);
      if (vectorIndex < 0) {
        throw new HopException("Failed to find source field " + srcFieldName + " in vector map");
      }
      FieldVector vector = vectors[vectorIndex];
      Object value = vector.getObject(rowNum);

      // XXX Temporary workaround because Arrow uses LocalDateTime, but Hop wants Date.
      if (value instanceof LocalDateTime) {
        value = new Date(((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli());
      }

      int outIndex = data.outputRowMeta.indexOfValue(outFieldName);
      if (outIndex < 0) {
        // Append new field
        outputRow[endOfRow] = value;
        endOfRow++;
      } else {
        // Replace existing field
        outputRow[outIndex] = value;
      }
    }

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
      case Date:
        return IValueMeta.TYPE_DATE;
      case Timestamp:
        return IValueMeta.TYPE_TIMESTAMP;
      default:
        throw new HopException("Arrow type '" + typeId + "' is not handled yet.");
    }
  }
}
