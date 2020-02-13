package io.mycat.router.migrate;

import io.mycat.router.NodeIndexRange;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by magicdoom on 2016/9/16.
 */
public class MigrateUtils {

    /**
     * 扩容计算，以每一个源节点到一个目标节点为一个任务
     *
     * @param integerListMap 会进行修改，所以传入前请自己clone一份
     * @param oldDataNodes
     * @param newDataNodes
     * @param slotsTotalNum
     * @return
     */
    public static SortedMap<String, List<MigrateTask>> balanceExpand(List<List<NodeIndexRange>> integerListMap, List<String> oldDataNodes, List<String> newDataNodes, int slotsTotalNum) {
        int newNodeSize = oldDataNodes.size() + newDataNodes.size();
        int newSlotPerNode = slotsTotalNum / newNodeSize;
        TreeMap<String, List<MigrateTask>> newNodeTask = new TreeMap<>();
        int remainder = slotsTotalNum - newSlotPerNode * (newNodeSize);
        for (int oldNodeIndex = 0; oldNodeIndex < integerListMap.size(); oldNodeIndex++) {
            List<NodeIndexRange> rangeList = integerListMap.get(oldNodeIndex);
            int needMoveNum = getCurTotalSize(rangeList) - newSlotPerNode;
            List<NodeIndexRange> allMoveList = getPartAndRemove(rangeList, needMoveNum);
            for (int newNodeIndex = 0; newNodeIndex < newDataNodes.size(); newNodeIndex++) {
                String newDataNode = newDataNodes.get(newNodeIndex);
                if (allMoveList.isEmpty()) {
                    break;
                }
                List<MigrateTask> curRangeList = newNodeTask.computeIfAbsent(newDataNode, s -> new ArrayList<>());
                int hasSlots = getCurTotalSizeForTask(curRangeList);
                int needMove = (newNodeIndex == 0) ? newSlotPerNode - hasSlots + remainder : newSlotPerNode - hasSlots;
                if (needMove > 0) {
                    List<NodeIndexRange> moveList = getPartAndRemove(allMoveList, needMove);
                    if (oldNodeIndex >= oldDataNodes.size()) {
                        throw new IllegalArgumentException();
                    }
                    curRangeList.add(new MigrateTask(oldDataNodes.get(oldNodeIndex), newDataNode, moveList));
                    newNodeTask.put(newDataNode, curRangeList);
                }
            }
            if (allMoveList.size() > 0) {
                throw new IllegalArgumentException("some slot has not moved to");
            }
        }
        return newNodeTask;
    }


    private static List<NodeIndexRange> getPartAndRemove(List<NodeIndexRange> rangeList, long size) {
        List<NodeIndexRange> result = new ArrayList<>();
        for (int i = 0; i < rangeList.size(); i++) {
            NodeIndexRange range = rangeList.get(i);
            if (range == null) {
                continue;
            }
            if (range.getSize() == size) {
                result.add(new NodeIndexRange(range.getNodeIndex(), range.getValueStart(), range.getValueEnd()));
                rangeList.set(i, null);
                break;
            } else if (range.getSize() < size) {
                result.add(new NodeIndexRange(range.getNodeIndex(), range.getValueStart(), range.getValueEnd()));
                size = size - range.getSize();
                rangeList.set(i, null);
            } else if (range.getSize() > size) {
                result.add(new NodeIndexRange(range.getNodeIndex(), range.getValueStart(), range.getValueStart() + size - 1));
                rangeList.set(i, new NodeIndexRange(range.getNodeIndex(), range.getValueStart() + size, range.getValueEnd()));
                break;
            }else {
                throw new IllegalArgumentException();
            }
        }
        for (int i = rangeList.size() - 1; i >= 0; i--) {
            NodeIndexRange range = rangeList.get(i);
            if (range == null) {
                rangeList.remove(i);
            }
        }
        return result;
    }

    private static int getCurTotalSizeForTask(List<MigrateTask> rangeList) {
        return rangeList.stream().mapToInt(task -> getCurTotalSize(task.getSlots())).sum();
    }

    private static int getCurTotalSize(List<NodeIndexRange> slots) {
        return slots.stream().map(i -> i.getSize()).mapToInt(i -> i.intValue()).sum();
    }


}
