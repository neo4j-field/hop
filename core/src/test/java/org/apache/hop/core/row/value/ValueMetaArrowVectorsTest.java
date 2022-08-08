package org.apache.hop.core.row.value;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.util.ArrowBufferAllocator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ValueMetaArrowVectorsTest {
    @BeforeClass
    public static void before() throws Exception {
        HopClientEnvironment.init();
    }

    @Test
    public void testCloneData() throws HopValueException {
        BufferAllocator allocator = ArrowBufferAllocator.rootAllocator();
        FieldVector[] vectors = new FieldVector[] {
            new IntVector("a", allocator), new IntVector("b", allocator),
        };

        for (FieldVector valueVector : vectors) {
            valueVector.allocateNewSafe();
            ((IntVector) valueVector).set(0, 42);
            valueVector.setValueCount(1);
        }

        ValueMetaArrowVectors valueMeta = new ValueMetaArrowVectors(
                "test",
                new Schema(Arrays
                        .stream(vectors)
                        .map(FieldVector::getField)
                        .collect(Collectors.toList())));

        FieldVector[] cloned = (FieldVector[]) valueMeta.cloneValueData(vectors);
        Assert.assertNotSame(vectors, cloned);
        Assert.assertEquals("a", cloned[0].getName());
        Assert.assertEquals("b", cloned[1].getName());

        for (FieldVector vector : vectors) {
            Assert.assertEquals(42, vector.getObject(0));
            vector.close();
        }
        for (FieldVector vector : cloned) {
            Assert.assertEquals(42, vector.getObject(0));
            vector.close();
        }
    }
}
