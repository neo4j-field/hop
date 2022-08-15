package org.apache.hop.arrow.transforms.arrowencode;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ArrowEncodeDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ArrowEncodeMeta.class;

  private final ArrowEncodeMeta input;

  private TextVar wOutputField;
  private TextVar wBatchSizeField;
  private TableView wFields;

  public ArrowEncodeDialog(
      Shell parent,
      IVariables variables,
      Object baseTransformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) baseTransformMeta, pipelineMeta, transformName);

    input = (ArrowEncodeMeta) baseTransformMeta;
  }

  @Override
  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ArrowEncodeDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.addListener(SWT.Selection, e -> getFields());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "ArrowEncodeDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Label wlOutputField = new Label(shell, SWT.RIGHT);
    wlOutputField.setText(BaseMessages.getString(PKG, "ArrowEncodeDialog.OutputField.Label"));
    props.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    fdlOutputField.top = new FormAttachment(lastControl, margin);
    wlOutputField.setLayoutData(fdlOutputField);
    wOutputField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOutputField.setText(transformName);
    props.setLook(wOutputField);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.top = new FormAttachment(wlOutputField, 0, SWT.CENTER);
    fdOutputField.right = new FormAttachment(100, 0);
    wOutputField.setLayoutData(fdOutputField);
    lastControl = wOutputField;

    Label wlBatchSize = new Label(shell, SWT.RIGHT);
    wlBatchSize.setText(BaseMessages.getString(PKG, "ArrowEncodeDialog.BatchSize.Label"));
    props.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wBatchSizeField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBatchSizeField.setText(String.valueOf(input.getBatchSize()));
    props.setLook(wBatchSizeField);
    FormData fdBatchSizeField = new FormData();
    fdBatchSizeField.left = new FormAttachment(middle, 0);
    fdBatchSizeField.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    fdBatchSizeField.right = new FormAttachment(100, 0);
    wBatchSizeField.setLayoutData(fdBatchSizeField);
    lastControl = wBatchSizeField;

    Label wlFields = new Label(shell, SWT.RIGHT);
    wlFields.setText(BaseMessages.getString(PKG, "ArrowEncodeDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] fieldsColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ArrowEncodeDialog.Fields.Column.SourceField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ArrowEncodeDialog.Fields.Column.TargetField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };

    wFields =
        new TableView(
            variables,
            shell,
            SWT.NONE,
            fieldsColumns,
            input.getSourceFields().size(),
            false,
            null,
            props);
    props.setLook(wFields);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    wOutputField.setText(Const.NVL(input.getOutputFieldName(), "arrow"));

    int rowNr = 0;
    for (SourceField sourceField : input.getSourceFields()) {
      TableItem item = wFields.table.getItem(rowNr++);
      int col = 1;
      item.setText(col++, Const.NVL(sourceField.getSourceFieldName(), ""));
      item.setText(col++, Const.NVL(sourceField.getTargetFieldName(), ""));
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    try {
      if (Utils.isEmpty(wTransformName.getText())) {
        return;
      }

      input.setOutputFieldName(wOutputField.getText());

      int batchSize = Const.toInt(wBatchSizeField.getText(), -1);
      if (batchSize < 1) {
        throw new HopValueException("Invalid batch size (" + batchSize + "). Value must be >= 1.");
      }
      input.setBatchSize(batchSize);

      input.getSourceFields().clear();
      for (TableItem item : wFields.getNonEmptyItems()) {
        String sourceField = item.getText(1);
        String targetField = item.getText(2);
        input.getSourceFields().add(new SourceField(sourceField, targetField));
      }

      transformName = wTransformName.getText(); // return value
      transformMeta.setChanged();
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Invalid configuration provided.", e);
    }
    dispose();
  }

  /** Add all the fields to the table view... */
  private void getFields() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      BaseTransformDialog.getFieldsFromPrevious(
          r,
          wFields,
          1,
          new int[] {1},
          new int[] {},
          -1,
          -1,
          new ITableItemInsertListener() {
            @Override
            public boolean tableItemInserted(TableItem tableItem, IValueMeta v) {
              String sourceFieldName = v.getName();
              String targetFieldName = v.getName();
              tableItem.setText(1, sourceFieldName);
              tableItem.setText(2, targetFieldName);

              return true;
            }
          });
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields", e);
    }
  }
}
