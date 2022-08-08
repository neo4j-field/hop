package org.apache.hop.core.row.value;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.util.ArrowBufferAllocator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ValueMetaArrowVectorTest {
    @BeforeClass
    public static void before() throws Exception {
        HopClientEnvironment.init();
    }

    @Test
    public void testCloneData() throws HopValueException {
        BufferAllocator allocator = ArrowBufferAllocator.rootAllocator();
        ValueVector[] vectors = new ValueVector[] {
            new IntVector("a", allocator), new IntVector("b", allocator),
        };

        for (ValueVector valueVector : vectors) {
            valueVector.allocateNewSafe();
            ((IntVector) valueVector).set(0, 42);
            valueVector.setValueCount(1);
        }

        ValueMetaArrowVector valueMeta = new ValueMetaArrowVector(
                "test",
                new Schema(Arrays
                        .stream(vectors)
                        .map(ValueVector::getField)
                        .collect(Collectors.toList())));

        ValueVector[] cloned = (ValueVector[]) valueMeta.cloneValueData(vectors);
        Assert.assertNotSame(vectors, cloned);
        Assert.assertEquals("a", cloned[0].getName());
        Assert.assertEquals("b", cloned[1].getName());

        for (ValueVector vector : vectors) {
            Assert.assertEquals(42, vector.getObject(0));
            vector.close();
        }
        for (ValueVector vector : cloned) {
            Assert.assertEquals(42, vector.getObject(0));
            vector.close();
        }
    }
}
