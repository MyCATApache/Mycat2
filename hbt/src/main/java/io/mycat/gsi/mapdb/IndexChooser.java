//package io.mycat.gsi.mapdb;
//
//import io.mycat.SimpleColumnInfo;
//
//import java.util.Collection;
//import java.util.Objects;
//
//@FunctionalInterface
//public interface IndexChooser {
//    /**
//     * 选择索引
//     * @param indexList 索引列
//     * @param columns 查询列
//     * @return
//     */
//    IndexStorage choseIndex(Collection<IndexStorage> indexList, SimpleColumnInfo[] columns);
//
//    /**
//     * 覆盖最多列
//     */
//    IndexChooser HIT_MAX_COLUMNS = (indexList, columns) -> {
//        int maxScore = 0;
//        IndexStorage result = null;
//        for (IndexStorage index : indexList) {
//            int score = 0;
//            SimpleColumnInfo[] columnInfos = index.getIndexInfo().getIndexes();
//            for (int i = 0; i < columnInfos.length && i<columns.length; i++) {
//                if(!Objects.equals(columnInfos[i].getColumnName(),columns[i].getColumnName())){
//                    break;
//                }
//                score = i;
//            }
//            if(result == null || score > maxScore){
//                result = index;
//            }
//        }
//        return result;
//    };
//
//
//}
