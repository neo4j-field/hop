package org.apache.hop.arrow.transforms.arrowencode;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.Text;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ArrowEncodeTest {
  private static TransformMockHelper<ArrowEncodeMeta, ArrowEncodeData> amh;

  private static Date date1;
  private static Date date2;

  @Before
  public void setup() {
    amh = new TransformMockHelper<>("ArrowEncode", ArrowEncodeMeta.class, ArrowEncodeData.class);
    when(amh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(amh.iLogChannel);
    when(amh.pipeline.isRunning()).thenReturn(true);

    date1 = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
    date2 = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
  }

  @After
  public void cleanUp() {
    amh.cleanUp();
  }

  @Test
  public void testEncodingSingleBatch() throws HopException {
    ArrowEncodeMeta meta = new ArrowEncodeMeta();
    meta.setSourceFields(
        List.of(
            new SourceField("age", "age_out"),
            new SourceField("name", "name_out"),
            new SourceField("dob", "dob_out"),
            new SourceField("last_login", "last_login_out")));
    ArrowEncodeData data = new ArrowEncodeData();

    ArrowEncode transform =
        new ArrowEncode(amh.transformMeta, meta, data, 0, amh.pipelineMeta, amh.pipeline);
    transform.init();
    transform.addRowSetToInputRowSets(mockInputRowSet());

    IRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    // We are testing 2 rows. Third call should be the end.
    //
    Assert.assertTrue(transform.processRow());
    Assert.assertTrue(transform.processRow());
    Assert.assertFalse("Should be done processing rows", transform.processRow());
    Assert.assertTrue(outputRowSet.isDone());

    Object[] row = outputRowSet.getRow();
    Assert.assertNotNull(row);
    int index = outputRowSet.getRowMeta().indexOfValue("arrow");
    Assert.assertTrue("Should have a non-zero index", index > 0);

    FieldVector[] vectors = (FieldVector[]) row[index];
    Assert.assertEquals("Should have 4 vectors", 4, vectors.length);

    FieldVector vector = vectors[0];
    Assert.assertEquals("Should be renamed age_out", vectors[0].getName(), "age_out");
    Assert.assertEquals(2, vector.getValueCount());
    Assert.assertEquals(42L, vector.getObject(0));
    Assert.assertEquals(10L, vector.getObject(1));

    vector = vectors[1];
    Assert.assertEquals("Should be renamed name_out", vectors[1].getName(), "name_out");
    Assert.assertEquals(2, vector.getValueCount());
    Assert.assertEquals("Forty Two", vector.getObject(0).toString());
    Assert.assertEquals("Ten", vector.getObject(1).toString());

    vector = vectors[2];
    Assert.assertEquals("Should be renamed dob_out", vectors[2].getName(), "dob_out");
    Assert.assertEquals(2, vector.getValueCount());
    Assert.assertEquals(date1.toInstant(), ((LocalDateTime) vector.getObject(0)).atOffset(ZoneOffset.UTC).toInstant());
    Assert.assertEquals(date2.toInstant(), ((LocalDateTime) vector.getObject(1)).atOffset(ZoneOffset.UTC).toInstant());

    vector = vectors[3];
    Assert.assertEquals("Should be renamed last_login_out", vectors[3].getName(), "last_login_out");
    Assert.assertEquals(2, vector.getValueCount());
    Assert.assertEquals(12345L * 1_000_000, vector.getObject(0));
    Assert.assertEquals(67890L * 1_000_000, vector.getObject(1));

    Arrays.stream(vectors).forEach(ValueVector::close);
  }

  private IRowSet mockInputRowSet() {
    IRowMeta inputRowMeta = new RowMeta();

    inputRowMeta.addValueMeta(0, new ValueMetaInteger("age"));
    inputRowMeta.addValueMeta(1, new ValueMetaString("name"));
    inputRowMeta.addValueMeta(2, new ValueMetaDate("dob"));
    inputRowMeta.addValueMeta(3, new ValueMetaTimestamp("last_login"));


    IRowSet inputRowSet = amh.getMockInputRowSet(
        new Object[][] {
            {42L, "Forty Two", date1, Timestamp.from(Instant.ofEpochMilli(12345))},
            {10L, "Ten",       date2, Timestamp.from(Instant.ofEpochMilli(67890))}});
    doReturn(inputRowMeta).when(inputRowSet).getRowMeta();

    return inputRowSet;
  }
}
