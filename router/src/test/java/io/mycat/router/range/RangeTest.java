package io.mycat.router.range;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RangeTest {


    @Test
    public void removeAndGetRemain(){
        List<Range> oldRangeList1= Lists.newArrayList(Range.create(0,51199));
        List<Range> newRangeList1=Lists.newArrayList(Range.create(0,20479),Range.create(20480,30719));
        List<Range> result1=Range.removeAndGetRemain(oldRangeList1,newRangeList1);
        Assert.assertEquals(1,result1.size());
        Assert.assertEquals(30720,result1.get(0).start());
        Assert.assertEquals(51199,result1.get(0).end());

        List<Range> oldRangeList2=Lists.newArrayList(Range.create(51200,102399));
        List<Range> newRangeList2=Lists.newArrayList(Range.create(61440,81919),Range.create(51200,61439));
        List<Range> result2=Range.removeAndGetRemain(oldRangeList2,newRangeList2);
        Assert.assertEquals(1,result2.size());
        Assert.assertEquals(81920,result2.get(0).start());
        Assert.assertEquals(102399,result2.get(0).end());

    }
    @Test
    public void removeAndGetRemain1(){
        List<Range> oldRangeList1=Lists.newArrayList(Range.create(0,0),Range.create(1,5),Range.create(6,40000),Range.create(40001,51199));
        List<Range> newRangeList1=Lists.newArrayList(Range.create(0,3),Range.create(20480,30719));
        List<Range> result1=Range.removeAndGetRemain(oldRangeList1,newRangeList1);
        Assert.assertEquals(4,result1.size());
        Assert.assertEquals(4,result1.get(0).start());
        Assert.assertEquals(5,result1.get(0).end());
        Assert.assertEquals(6,result1.get(1).start());
        Assert.assertEquals(20479,result1.get(1).end());
        Assert.assertEquals(30720,result1.get(2).start());
        Assert.assertEquals(40000,result1.get(2).end());
        Assert.assertEquals(40001,result1.get(3).start());
        Assert.assertEquals(51199,result1.get(3).end());



    }

}