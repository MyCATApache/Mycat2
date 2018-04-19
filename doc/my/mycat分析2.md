MergeCol: 聚合方法
ColMeta:聚合方法以及聚合类型
RowDataPacketGrouper:数据汇聚类

mycat结果集合并主要有

我们这里分析堆内即DataMergeService，他是一个实现了Runnable接口的类，这里看到，只有操作了三个方法onRowMetaData，onNewRecord，outputMergeResult，其中只有onRowMetaData是自己实现的，其他两个都是父类AbstractDataNodeMerge的。我们重点看onRowMetaData这个方法。
这个方法有两个参数columToIndx，fieldCount，分别为列的集合，字段数量，这个方法调用了		             
       grouper = new  RowDataPacketGrouper(groupColumnIndexs,
					mergCols.toArray(new MergeCol[mergCols.size()]),
					rrs.getHavingCols());
初始化好了数据汇聚类
那接下来我们看onNewRecord,这个方法主要是处理新进来每个row数据，通过PackWraper进行封装，该方法调用了addPack这个方法。在这个方法里面


传入的三个参数分别为要group的列，聚合方法，还有having的列。展开这个数据汇聚类，里面有一个主要的getResult方法，该方法首先判断是否有聚合函数，有的话调用mergAvg方法，接着判断是否有having列没有的话调用filterHaving方法。
那getResult在哪里被调用的呢，我们刚才说到DataMergeService是个线程实现类，getResult就是在那里面调用的，
