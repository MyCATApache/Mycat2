package io.mycat.mushroom;

public abstract class LikeExpression <VFrame> implements CompiledSQLExpression<VFrame>  {

    //check last null ,first null
    @Override
    public Object eval(VFrame vFrame) {
        Comparable o = (Comparable)readByIndex(vFrame, 0);
        Comparable o1 = (Comparable)readByIndex(vFrame, 1);
        return o.compareTo(o1)<=0;
    }
}
