package io.mycat.router.range;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class IntEnumeratorTest extends TestCase {

    @Test
    public void test() {
        IntEnumerator e = IntEnumerator.ofInt(1, 12, 1, false, 12);


        for (int i = 1; i < 12; i++) {
            Optional<Iterable<Integer>> integers = e.rangeClosed(i, i + 1);
            if (!integers.isPresent()) {
                System.out.println();
            }
            Assert.assertTrue(integers.isPresent());
        }
        Optional<Iterable<Integer>> integers = e.rangeClosed(0, 1);
        Assert.assertFalse(integers.isPresent());

        integers = e.rangeClosed(12, 13);
        Assert.assertFalse(integers.isPresent());

        Assert.assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
                e.rangeClosedAsList(1, 12).get().toString());

    }

    @Test
    public void test2() {
        IntEnumerator e = IntEnumerator.ofInt(0, 11, 1, false, 12);


        for (int i = 0; i < 11; i++) {
            Optional<Iterable<Integer>> integers = e.rangeClosed(i, i + 1);
            if (!integers.isPresent()) {
                System.out.println();
            }
            Assert.assertTrue(integers.isPresent());
        }
        Optional<Iterable<Integer>> integers = e.rangeClosed(-1, 0);
        Assert.assertFalse(integers.isPresent());

        integers = e.rangeClosed(11, 12);
        Assert.assertFalse(integers.isPresent());

        Assert.assertEquals("[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]",
                e.rangeClosedAsList(0, 11).get().toString());

    }

    @Test
    public void test3() {
        IntEnumerator e = IntEnumerator.ofInt(1, 12, 1, true, 12);


        for (int i = 1; i < 12; i++) {
            Optional<Iterable<Integer>> integers = e.rangeClosed(i, i + 1);
            if (!integers.isPresent()) {
                System.out.println();
            }
            Assert.assertTrue(integers.isPresent());
        }
        Optional<Iterable<Integer>> integers = e.rangeClosed(0, 1);
        Assert.assertFalse(integers.isPresent());

        integers = e.rangeClosed(12, 13);
        Assert.assertFalse(integers.isPresent());

        Assert.assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
                e.rangeClosedAsList(1, 12).get().toString());


        Assert.assertEquals("[12, 1]",
                e.rangeClosedAsList(12, 1).get().toString());

        Assert.assertEquals("[12, 1, 2]",
                e.rangeClosedAsList(12, 2).get().toString());
        System.out.println();

    }


    @Test
    public void test4() {
        IntEnumerator e = IntEnumerator.ofInt(1, 12, 1, true, 6);


        for (int i = 1; i < 12; i++) {
            Optional<Iterable<Integer>> integers = e.rangeClosed(i, i + 1);
            if (!integers.isPresent()) {
                System.out.println();
            }
            Assert.assertTrue(integers.isPresent());
        }
        Optional<Iterable<Integer>> integers = e.rangeClosed(0, 1);
        Assert.assertFalse(integers.isPresent());

        integers = e.rangeClosed(12, 13);
        Assert.assertFalse(integers.isPresent());

        Assert.assertFalse(      e.rangeClosedAsList(1, 12).isPresent());
        Assert.assertTrue(      e.rangeClosedAsList(1, 6).isPresent());
        Assert.assertFalse(      e.rangeClosedAsList(1, 8).isPresent());

    }
}