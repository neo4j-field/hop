package org.apache.hop.arrow.transforms.arrowencode;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaArrowVectors;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ArrowEncode",
    name = "Arrow Encode",
    description = "Encodes Hop fields into an Arrow Vector typed field",
    image = "arrow_encode.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/arrow-encode.html",
    keywords = "i18n::ArrowEncodeMeta.keyword")
public class ArrowEncodeMeta extends BaseTransformMeta<ArrowEncode, ArrowEncodeData> {
  private static final Class<?> PKG = ArrowEncodeMeta.class;

  @HopMetadataProperty(key = "output_field")
  private String outputFieldName = "arrow";

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<SourceField> sourceFields = List.of();

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String transformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    try {
      Schema schema = createArrowSchema(rowMeta, sourceFields);
      ValueMetaArrowVectors valueMeta =
          new ValueMetaArrowVectors(variables.resolve(outputFieldName), schema);
      rowMeta.addValueMeta(valueMeta);
    } catch (Exception e) {
      throw new HopTransformException(
          "Error creating Arrow schema and/or determining output field layout", e);
    }
  }

  public Schema createArrowSchema(IRowMeta inputRowMeta, List<SourceField> sourceFields)
      throws HopException {
    List<Field> arrowFields = new ArrayList<>(sourceFields.size());

    for (int i = 0; i < sourceFields.size(); i++) {
      IValueMeta valueMeta = inputRowMeta.getValueMeta(i);
      String name = sourceFields.get(i).calculateTargetFieldName();

      ArrowType type;
      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_INTEGER:
          // TODO int field precision and sign?
          type = new ArrowType.Int(64, true);
          break;
        case IValueMeta.TYPE_BOOLEAN:
          type = new ArrowType.Bool();
          break;
        case IValueMeta.TYPE_NUMBER:
          type = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
          break;
        case IValueMeta.TYPE_STRING:
          type = new ArrowType.Utf8();
          break;
        case IValueMeta.TYPE_TIMESTAMP:
          type = new ArrowType.Timestamp(
              TimeUnit.NANOSECOND,
              valueMeta.getDateFormatTimeZone().getDisplayName());
          break;
        case IValueMeta.TYPE_DATE:
          type = new ArrowType.Date(DateUnit.MILLISECOND);
          break;
        default:
          throw new HopException(
              "Writing Hop data type '" + valueMeta.getTypeDesc() + "' to Arrow is not supported");
      }

      // Nested types (i.e. with children) are not currently supported.
      //
      arrowFields.add(new Field(name, FieldType.nullable(type), null));
    }

    return new Schema(arrowFields);
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void setOutputFieldName(String outputFieldName) {
    this.outputFieldName = outputFieldName;
  }

  public List<SourceField> getSourceFields() {
    return sourceFields;
  }

  public void setSourceFields(List<SourceField> sourceFields) {
    this.sourceFields = sourceFields;
  }
}
