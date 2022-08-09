package org.apache.hop.arrow.transforms.arrowencode;

import org.apache.arrow.vector.*;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.ArrowBufferAllocator;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class ArrowEncode extends BaseTransform<ArrowEncodeMeta, ArrowEncodeData> {

  private int batchSize = 10_000;

  /**
   * Encode Hop Rows into an Arrow RecordBatch of Arrow Vectors.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta
   * @param data          the data object to store temporary data, database connections, caches, result sets,
   *                      hashtables etc.
   * @param copyNr        The copynumber for this transform.
   * @param pipelineMeta  The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline      The (running) pipeline to obtain information shared among the transforms.
   */
  public ArrowEncode(
      TransformMeta transformMeta,
      ArrowEncodeMeta meta,
      ArrowEncodeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();

    // Last row?
    if (row == null) {
      if (data.count > 0) {
        flush();
      }
      setOutputDone();
      return false;
    }

    // Either we're operating on our first row or the start of a new batch.
    //
    if (first) {
      first = false;

      // Initialize output row.
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.sourceFieldIndexes = new ArrayList<>();

      // Index the selected fields.
      //
      for (SourceField field : meta.getSourceFields()) {
        int index = getInputRowMeta().indexOfValue(field.getSourceFieldName());
        if (index < 0) {
          throw new HopException("Unable to find input field " + field.getSourceFieldName());
        }
        data.sourceFieldIndexes.add(index);
      }

      // Build the Arrow schema.
      //
      data.arrowSchema = meta.createArrowSchema(getInputRowMeta(), meta.getSourceFields());
      if (log.isDetailed()) {
        log.logDetailed("Schema: " + data.arrowSchema);
      }

      initializeVectors();
      data.batches = 0;
    }

    // Add Row to the current batch of Vectors
    append(row);

    // Flush and reinitialize if we're at the limit.
    //
    if (data.count == batchSize) {
      flush();
      initializeVectors();
    }

    return true;
  }

  private void initializeVectors() {
    data.vectors = data.arrowSchema
            .getFields()
            .stream()
            .map(field -> {
              FieldVector vector = field.createVector(ArrowBufferAllocator.rootAllocator());
              vector.allocateNewSafe();
              return vector;
            })
            .toArray(FieldVector[]::new);
    data.count = 0;
  }

  private void append(Object[] row) throws HopValueException {
    IRowMeta rowMeta = getInputRowMeta();

    for (int index : data.sourceFieldIndexes) {
      FieldVector vector = data.vectors[index];

      // XXX The mess...
      // TODO: Arrow List support
      if (vector instanceof IntVector) {
        ((IntVector) vector).set(index, rowMeta.getInteger(row, index).intValue());
      } else if (vector instanceof BigIntVector) {
        ((BigIntVector) vector).set(index, rowMeta.getInteger(row, index));
      } else if (vector instanceof Float4Vector) {
        ((Float4Vector) vector).set(index, rowMeta.getNumber(row, index).floatValue());
      } else if (vector instanceof Float8Vector) {
        ((Float8Vector) vector).set(index, rowMeta.getNumber(row, index));
      } else if (vector instanceof VarCharVector) {
        ((VarCharVector) vector).set(index, rowMeta.getString(row, index)
                .getBytes(StandardCharsets.UTF_8));
      } else {
        throw new HopValueException(this + " - encountered unsupported vector type: " + vector.getClass());
      }
    }
    data.count++;
  }

  private void flush() throws HopTransformException {
    // Finalize the vectors by setting their value counts
    Arrays.stream(data.vectors).forEach(v -> v.setValueCount(data.count));

    // Send the vectors on their merry way
    Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
    outputRow[getInputRowMeta().size()] = data.vectors;
    data.vectors = new FieldVector[] {};
    putRow(data.outputRowMeta, outputRow);

    logBasic("encoded " + data.count + " rows into Arrow Vectors");

    // Prepare for the next batch
    data.count = 0;
    data.batches++;
  }
}
