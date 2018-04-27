package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * jamie 2018.4.26
 *
 * @todo change String to long to reduce gc
 */
public class MergeAnnotation {
    private BufferSQLContext context;
    private List<Integer> dataNodes;
    private List<Integer> groupColumns;
    private List<Integer> mergeColumns;
    private int mergeType;
    //having
    private int left;
    private int op;
    private int right;
    //sort
    private List<Integer> orderColumns;
    private int orderType;
    private long limitStart;
    private long limitSize;


    public MergeAnnotation(BufferSQLContext context) {
        this.dataNodes = new ArrayList<>();
        this.groupColumns = new ArrayList<>();
        this.mergeColumns = new ArrayList<>();
        this.mergeType = 0;
        this.left = 0;
        this.op = 0;
        this.right = 0;
        this.orderColumns = new ArrayList<>();
        this.orderType = 0;
        this.limitStart = 0L;
        this.limitSize = 0L;
        this.context = context;
    }

    public boolean hasGroupBy() {
        return groupColumns != null && !groupColumns.isEmpty();
    }

    public boolean hasHaving() {
        return op != 0;
    }

    public boolean hasOrder() {
        return orderColumns != null && !orderColumns.isEmpty();
    }

    public String[] getDataNodes() {
        return map(dataNodes);
    }

    public String[] getGroupColumns() {
        return map(groupColumns);
    }

    public String[] getMergeColumns() {
        return map(mergeColumns);
    }

    public String getMergeType() {
        return get(mergeType);
    }

    public void setMergeType(int mergeType) {
        this.mergeType = mergeType;
    }

    public String getLeft() {
        return get(left);
    }

    public void setLeft(int left) {
        this.left = left;
    }

    public String getOp() {
        return get(op);
    }

    public void setOp(int op) {
        this.op = op;
    }

    public String getRight() {
        return get(right);
    }

    /////////////set////////////////

    public void setRight(int right) {
        this.right = right;
    }

    public String[] getOrderColumns() {
        return map(orderColumns);
    }

    public boolean isAsc() {
        return orderType == IntTokenHash.ORDER_ASC;
    }

    public long getLimitStart() {
        return limitStart;
    }

    public void setLimitStart(long limitStart) {
        this.limitStart = limitStart;
    }

    public long getLimitSize() {
        return limitSize;
    }

    public void setLimitSize(long limitSize) {
        this.limitSize = limitSize;
    }

    public void addDataNode(int pos) {
        this.dataNodes.add(pos);
    }

    public void addGroupColumn(int groupColumn) {
        this.groupColumns.add(groupColumn);
    }

    public void addMergeColumn(int mergeColumn) {
        this.mergeColumns.add(mergeColumn);
    }

    public void addOrderColumn(int orderColumn) {
        this.orderColumns.add(orderColumn);
    }

    public void setOrderType(int orderType) {
        this.orderType = orderType;
    }

    public void clear() {
        this.dataNodes.clear();
        this.groupColumns.clear();
        this.mergeColumns.clear();
        this.mergeType = 0;
        this.left = 0;
        this.op = 0;
        this.right = 0;
        this.orderColumns.clear();
        this.orderType = 0;
        this.limitStart = 0L;
        this.limitSize = 0L;
    }

    private String[] map(List<Integer> list) {
        ByteArrayInterface byteArrayInterface = context.getBuffer();
        HashArray hashArray = context.getHashArray();
        int size = list.size();
        String[] res = new String[size];
        for (int i = 0; i < size; i++) {
            res[i] = byteArrayInterface.getStringByHashArray(list.get(i), hashArray);
        }
        return res;
    }

    private String get(int pos) {
        ByteArrayInterface byteArrayInterface = context.getBuffer();
        return byteArrayInterface.getStringByHashArray(pos, context.getHashArray());
    }

    @Override
    public String toString() {
        return "MergeAnnotation{" +
                "context=" + context +
                ", dataNodes=" + Arrays.toString(this.getDataNodes()) +
                ", groupColumns=" + Arrays.toString(this.getGroupColumns()) +
                ", mergeColumns=" + Arrays.toString(this.getMergeColumns()) +
                ", mergeType=" + this.getMergeType() +
                ", left=" + this.getLeft() +
                ", op=" + this.getOp() +
                ", right=" + this.getRight() +
                ", orderColumns=" + Arrays.toString(this.getOrderColumns()) +
                ", isAsc=" + this.isAsc() +
                ", limitStart=" + this.getLimitStart() +
                ", limitSize=" + this.getLimitSize() +
                '}';
    }
}
