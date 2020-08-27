# optimizer-Mycat2.0

## Mycat2优化器

author:chenjunwen 2020-8-20



[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).



## 前言

​		本文描述的设计细节，大部分已经实现，有小部分没有完全实现。		

​		Mycat2减少使用人类理解编写的SQL解析路由和基于经验制作的查询引擎的思路来实现查询引擎,取而代之,使用基于关系表达式转换SQL的思路以支持更多甚至是标准SQL，MySQL语法的SQL。使用Java生态Apache Calcite项目,改造成以接收MySQL方言的SQL,翻译该SQL变成Mycat算子和查询物理库的SQL。该引擎在设计上可以脱离网络层独立运行。

​		Mycat2使用calcite作为优化器实现,优化有四个阶段。

​		第一阶段Mycat把MySQL语法的SQL编译成语法抽象树,并进一步把它编译成逻辑关系表达式，值得注意的是，辑MySQL方言元素以Hint或者特征的形态保存在表达式内，而不会以表达式表示。

​		第二阶段是基于规则改写逻辑关系表达式,把可以下推到Mysql关系表达式转换以特殊的节点保存,该节点最终执行的时候以SQL表示运算。

​		第三阶段是使用基于代价的优化方法把未能下推的,需要在Mycat里运算的算子选择物理算子。

​		第四阶段则进一步把物理算子翻译成对应的执行计划,执行计划是以树节点表示的执行树,与执行流程是对应的,而且是有状态的.而上述的关系表达式是不可变对象,无状态的。上述有微妙的关系，算子与执行器没有严格的对应关系，它们的树形状并不是对应的，但是有转换程序把算子生成执行器。

​		在Mycat2开发的历史中,使用calcite实现的查询引擎有两个版本,其技术特征如下。

### 第一代查询引擎

​		1.基于逻辑表结合分片条件翻译为类似union all语义和多个物理表

​		2.在合适的条件下,上拉Union All算子,相对地,其它在Union All算子之上的算子会下推

​		3.从叶子节点开始往上遍历(后序遍历),计算每个节点的分片属性,把相同分片属性的节点的根节点记录,然后把它向下遍历部变成SQL。

​		该设计最大程度依赖calcite现有功能,而且下推的方法比较直观，但是仅仅在逻辑关系表达式层面做处理,未能深入更深入的优化，需要编写更多规则实现，而且对于一些情况难以控制关系表达式的改写，另外由于开发时候属于早期阶段，未能实现Mycat自研的执行器所以未能使用执行成本优化,使用calcite的内置执行器实现执行。它在用户测试以及使用中发现使用Calcite的解释器执行器执行效率较低，而代码生成器的执行器难以理解调试。所以开始制作第二代执行引擎。

### 第二代查询引擎				

​		开发第二代查询引擎的过程中，尝试了很多方案，包括移植1.6的查询引擎，自研SQL编译成不支持关联子查询的表达式树并自研执行器，经过多次方案和测试，保留自研执行器。因为难以界定简单sql与复杂sql,可能导致多次编译,所以抛弃自研的SQL编译器。为了实现执行器，深入使用Calcite，实现MycatRel，把关系表达式转换为Mycat自研的关系表达式类，在此之上再转换成执行器。执行器保留Caclite的表达式编译器,使用代码生成技术,对标量表达式生成java代码，然后动态编译成类，然后加载为对象，执行。对于关系表达式，则是自己实现执行器，而不使用代码生成方案，虽然效率有所降低，但是便于调试，因为考虑到之后可能有替换执行器的可能，所以暂时无需优化至极致。

​		然后为了实现对物理节点的SQL可控，研究关系表达式与SQL的生成规则，实现了有效的关系表达式下推判断器。然后为了对已排序的节点进行规则优化，引入排序特征传播（相比之下，手动的HBT就比较难表达这些信息了，Hint可能是一个方向）。后来再参考阿里DRDS的参数化思路,实现参数化功能原型.最后为了结合第一代查询引擎的任意分配配置,基于名字而不是基于下标的分表映射方案(第二代原始设计是基于下标指向拆分的物理表),重新引入第一代查询引擎的思路,基于规则判断,对于复杂的SQL,逻辑表编译成物理表再优化。这样就完成了第二代查询引擎。

#### 引入参数化

​		理想的参数化就是把sql中的直接量提取出来,用参数化标记?替代,sql就变成参数化模板，把SQL中**部分**常量以?替换,形成参数化模板与参数值,以该参数化模板为key,查询缓存是否已经有物理算子或者已经解析好的SQL抽象语法树,也就说缓存的对象有两种.对于物理算子,一个参数化模板可能对应多种物理算子,这是因为使用的优化方法不同导致的结果.Mycat会使用执行代价的物理算子来执行。上面说得很理想，但是还是有限制的，在Mycat2里面，暂时不会对offset，limit，以及select item的值进行参数化。前者会影响生成物理算子，因为行数对算子选择有影响。后者会影响结果集类型的生成。

#### 参数化的MycatView

​		MycatView这个名字参考阿里DRDS的View算子，在Mycat2第一代引擎有类似的东西，叫做MycatTransientSQLTableScan,一种临时的TableScan,它以sql字符串和数据源信息来构造。而MycatView使用关系表达式与数据分布来表示，数据分布包含两类，一种是具体的，他们就是已经包含具体物理表的信息。一种是需要赋以参数值才可以得到具体信息，如果不，只能得到全表扫描信息，也就是说，MycatView计算物理表可以是惰性计算的。理想情况下，一个参数化的关系表达式，不同的参数值能影响它扫描分表数量。

#### 一对多的MycatView

​		一般来说，MycatView保存的关系表达式是关于逻辑表的，而物理表信息以数据分布来表示。在分表情况下，一般来说，一个逻辑表对应多个物理表。在实现了参数化后,关于逻辑表的关系表达式成为了模板的存在，（它也包含参数标记？）在生成执行计划的时候,应用参数值,使逻辑表的MycatView扩展成多个分片物理表的关系表达式。换句话说，使用MycatView‘概括’多个分片节点的关系表达式,即使用一个分片表达式与多个分片节点信息,减少规则重写表达式存在多个重复的改写消耗,设想,如果有100个物理表,使用逻辑表编译成物理表的方法,如果涉及对这些物理表的关系表达式改写则要进行100次,而使用MycatView‘概括’只需1次。

#### 具体的MycatView

​		而对于复杂的sql,Mycat可能会选择使用物理表扩展的方法来优化,这是因为分片条件对算子的形状影响很大,对于这个情况,mycat仅仅缓存sql抽象语法树,并在规则优化阶段就应用参数值,把逻辑表扩展成物理表。

#### 过早优化的MycatView

​		对于简单sql,如果在规则优化阶段已经得到MycatView,则无需进行物理算子优化,直接生成执行计划,因为MycatView本身就是物理算子。与第一代查询引擎相比,Mycat2自研了执行器,执行器与物理算子存在多对一的关系,例如就算Sort节点被代价优化器选择为MemSort,Mycat在生成执行器的时候发现带有limit和排序字段会编译成TopN执行器,而发现只有limit时候则会仅限制行数量。

#### 参数化与预处理语句

​		在一般的预处理语句,用户提供预处理语句,预处理语句通过特殊的语法或者客户端使用特殊通讯协议把预处理语句发送到数据库,然后数据库返回指向该预处理语句的句柄.当客户端要执行这个预处理语句的时候,通过对?赋值,然后交给数据库执行.预处理语句中的?是一种参数标记,一般来说,参数是直接量,它们的参数类型可以根据SQL的语法,语义推导出来出来,但是不一定完全是确定的,因为实际参数类型取决于实际值.对于整个SQL的结果集类型,是根据Select Item表达式的类型推导出来的.如此来说,整个结果集的类型实际上也是不确定的.

#### 外部参数化

​		对于客户端相关的预处理语句参数,在Mycat的查询引擎中称为外部参数,它们就是直接量.对于把没有?参数化标记的SQL转换成带有?和参数值的SQL对象,这个过程称为参数化.在这里我们把外部参数相关的参数化叫做外部参数化。

#### 内部参数化

​		在SQL层面上,不同的SQL产生的逻辑关系表达式可能是相似的.把TableScan,Values的算子去掉,它们是相同的。如果把关系表达式也用？来表示，则称为内部参数化，如果以SQL模板来表示内部化参数则表名也是？。这样的SQL模板更为通用。Mycat暂时没有显式实现这种方式但有使用这种思路。

#### 不参数化的直接量

​		1.对物理算子的形态有很大影响的,比如limit的参数,而且limit的参数可以计算结果集的行数,会影响物理算子的选择,所以此类的参数化止步于物理算子生成之前。

​		2.表名,Mycat2没有进一步把表名用参数化标记表示,而保留逻辑表来表示表信息,当生成执行器的时候,逻辑表信息会被替换成物理表

​		3.and/or/in表达式,Mycat2也没有把and/or的条件中的直接量以数组的形式排列,而选择保持原样。

​		4.select item中的直接量

#### 关联子查询		

​		Calcite在编译SQL的时候会解子查询，使用join，SemiJoin，Agg替代关联子查询，最后还有小部分子查询不能消去，以Correlate的形式存在，对于这个算子，Mycat2的执行器暂时不支持（如果以后这种sql确实有很多需求就考虑支持）

#### 内置表

​		Mycat2支持自定义表数据的来源，具体要自定义开发

​								

### 第二代查询引擎执行流程

1.SQL解析成语法抽象树->转换成逻辑关系表达式

2.检查SQL特征,选择优化方式

优化方式1

​	下推Filter

​	结合Filter与逻辑表扩展成物理表

​	上拉Union All语义

优化方式2

​	下推Filter条件同时生成MycatView

3.选择物理算子

4.生成执行器并执行



​		由于Mysql本身对于一些语法执行效率不高,Mycat在第一代查询引擎与第二代查询引擎都有选择性地(~~不下推~~)下推集合操作来减少Mysql执行这些sql的效率低下。在第一代查询引擎中,从叶子节点开始后序遍历,记录相同分片目标的节点,然后再次后序遍历,直到遇上不满足下推条件的节点,然后把它的子节点翻译成SQL。

​		相似地,在第二代查询引擎中也有类似逻辑,但是不像第一代引擎是有独立的翻译阶段,而是与其他RBO规则一起在相同的优化器里进行规则转换,,同时转换时候可以传播排序信息,如果遇上能转换为MegreSort的条件,就会马上转换,而无需在CBO阶段中完成.若果MycatView阶段在RBO阶段已经被提升到根部,那也无需进行CBO了.当RBO完成的时候,根节点不是物理算子,会进行CRO进一步转换成物理算子(第一代查询引擎没有这个阶段),通过计算节点特征和行数等统计信息来选择物理计划。



#### MycatView的生成规则

设计目标是尽量生成简单的SQL,同时让后端数据库(Mysql)执行一定量的运算,总体来说,算子应该区分二阶段

以MycatView为边界,MycatView之上的算子为Mycat2执行的算子,MycatView之下为MySQL要执行的运算.MycatView之上的算子能否下推到MycatView之下,一般来说有两个规则

1.如果MycatView之上的节点放在MycatView之下的节点的根节点之上,是否会导致整体算子运算执行效率降低,尤其是MySQL上的执行.

2.MycatView之下的算子的类型集合是否已经包含MycatView之上的算子类型,如果不存在,则一般可以下推,否则参考下一条规则

3.MycatView之下的算子从叶子节点向根节点遍历所得的关系表达式的顺序是否是自然的,这条规则的另一种表达是,MycatView之下的关系表达式对应的SQL无需以子查询来表达,特殊情况如下

4.对于部分能通过SQL表达的简化关联子查询在Mycat中执行的下推,直接下推到MycatView

5.对于join 全局表,直接下推到MycatView







#### RBO规则化转换

基于两个节点的下推判断,?指任意关系表达式



##### TableScan

TableScan=>MycatView(TableScan)



##### Filter

Filter(MycatView(Project(?)))≠>

Filter(MycatView(Set(?)))≠>

Filter(MycatView(Agg(?)))=>MycatView(Filter(Agg(?)):group by having语法

Filter(MycatView(Sort(?)))≠>

Filter(MycatView(Filter(?)))≠>MycatView(Filter(?)):需要MycatView之上合拼Filter

Filter(MycatView(Join(?)))=>MycatView(Filter(Join(?)))

Filter(MycatView(TableScan(?)))=>MycatView(Filter(TableScan(?)))





##### Project

Project(MycatView(Project(?)))=>MycatView(Project(Project(?)):尽量合拼Project

Project(MycatView(Set(?)))≠>

Project(MycatView(Agg(?)))=>MycatView(Project(Agg(?))

Project(MycatView(Sort(?)))≠>

Project(MycatView(Filter(?)))=>MycatView(Project(Filter(?)))

Project(MycatView(Join(?)))=>MycatView(Project(Join(?)))

Project(MycatView(TableScan(?)))=>MycatView(Project(TableScan(?)))



##### Join

Join(MycatView(Project(?)))≠>

Join(MycatView(Set(?)))≠>

Join(MycatView(Agg(?)))≠>

Join(MycatView(Sort(?)))≠>

Join(MycatView(Filter(?)))≠>

Join(MycatView(Join(?)))=>MycatView(Join(Join(?)))

Join(MycatView(TableScan(?)))=>MycatView(Join(TableScan(?)))



##### Agg

Agg(MycatView(Project(?)))=>MycatView(Agg(Project(?)))

Agg(MycatView(Set(?)))≠>

Agg(MycatView(Agg(?)))≠>

Agg(MycatView(Sort(?)))≠>

Agg(MycatView(Filter(?)))=>MycatView(Agg(Filter(?)))

Agg(MycatView(Join(?)))=>MycatView(Agg(Join(?)))

Agg(MycatView(TableScan(?)))=>MycatView(Agg(TableScan(?)))



##### Sort

Sort(MycatView(Project(?)))=>MycatView(Sort(Project(?)))

Sort(MycatView(Set(?)))≠>

Sort(MycatView(Agg(?)))=>MycatView(Sort(Agg(?)))

Sort(MycatView(Sort(?)))≠>

Sort(MycatView(Filter(?)))=>MycatView(Sort(Filter(?)))

Sort(MycatView(Join(?)))=>MycatView(Sort(Join(?)))

Sort(MycatView(TableScan(?)))=>MycatView(Sort(TableScan))



##### Set

Set(MycatView(Project(?)))≠>

Set(MycatView(Set(?)))≠>

Set(MycatView(Agg(?)))≠>

Set(MycatView(Sort(?)))≠>

Set(MycatView(Filter(?)))≠>

Set(MycatView(Join(?)))≠>

Set(MycatView(TableScan(?)))=>MycatView(Set(TableScan))



##### Correlate

Correlate(MycatView(Project(?)))≠>

Correlate(MycatView(Set(?)))≠>

Correlate(MycatView(Agg(?)))≠>

Correlate(MycatView(Sort(?)))≠>

Correlate(MycatView(Filter(?)))≠>

Correlate(MycatView(Join(?)))≠>

Correlate(MycatView(TableScan(?)))=>MycatView(Correlate(TableScan))







### Mycat物理算子

HBT执行也是使用这套物理算子



##### MycatProject

1.执行投影操作

2.执行表达式计算

3.流式



##### MycatFilter

1.执行过滤操作

2.流式



##### MycatNestedLoopJoin

1.当其他join实现无法被选择的时候使用

2.左侧源流式,右侧数据源支持可重复计算或者使用临时表缓存计算结果

3.两重循环计算



##### MycatNestedLoopSemipJoin

SemiJoin版本的NestedLoopSemipJoin



##### MycatHashJoin

1.用于等值连接的Join

2.左侧源流式,右侧拉取所有数据



##### MycatSemiHashJoin

SemiJoin版本的MycatHashJoin



##### MycatBatchNestedLoopJoin

(一般关闭,因为优化器暂时没有根据左侧数据源得到行统计)

1.用于左侧数据源的结果数量较少的情况,右侧是MycatView关系表达式,Join有显式的关联键

2.左侧源流式,右侧源需要是MycatView(转换成LookupExecutor),此MycatView在JDBC数据源可以重复从左侧源得到参数值应用并生成SQL执行,支持涉及多个分片。



##### MycatSortMergeJoin

1.Join有显式的等价关联键,并且是inner join.此时Join的两个源的已经按照Join key排序,则应用MycatSortMergeJoin规则

2.两侧源流式处理

~~3.输出结果已排序~~



##### MycatMergeSemiJoin

SemiJoin版本的MycatHashJoin



##### MycatHashAgg

1.当其他Agg实现无法被选择的时候使用



##### MycatSortAgg

1.当源已对需要聚合的字段排序,则使用MycatSortAgg替代MycatHashAgg



##### MycatMemSort

1.在当无法使用其他排序算子的时候使用

2.当只有排序字段的时候,而没有offset,limit的时候,在生成执行器时候会把数据保存到内存然后再排序

3.当只有offset,limit的时候,会进行流式计算偏移与行数等价于MycatLimit算子

4.当同时有排序字段,offset,limit的时候,等价于MycatTopN算子,在生成执行器的时候使用TopN执行器



##### MycatTopN

1.当逻辑关系表达式具有排序字段,offset,limit的时候,应用MycatTopN算子

2.固定内存占用空间



##### MycatMegreSort

1.当MycatView是已经排序的时候,而且MycatView内的算子是设计多个分片的,则使用MycatMegreSort合拼这些分片的数据

2.强制使用MycatSortMergeJoin或者MycatSortAgg需要添加Sort算子,则该sort算子会被转换为MycatMegreSort

~~3.当出现Sort(UnionAll(Sort(...),Sort(...)))都对相同的字段排序的时候,会转换成MycatMegreSort(UnionAll(Sort(...),Sort(...)))~~



##### MycatUnion

1.union all的时候,使用流式合拼,对数据源依次求值

2,union distinct的时候使用集合语义,把数据拉取到内存里运算



##### MycatMinus

1.求差,使用集合语义,把数据拉取到内存里面运算,mysql语法不支持这个算子,HBT使用



##### MycatIntersect

1.求交集,使用集合语义,把数据拉取到内存里面运算,mysql语法不支持这个算子,HBT使用



##### MycatValues

1.针对逻辑关系表达式中产生的小型临时表,or/in表达式产生的临时表,集合运算产生的临时表使用MycatValues表示,其中HBT也用到了该算子

2.MycatValues等价与没有表信息的表,具有字段信息和在内存存放的行数据(字面量)



##### MycatInsert

1.分表表专用插入算子,把sql参数化之后得出sql模板,与相应的表信息

2.MycatInsert在生成执行器的时候应用参数值,并计算分片节点信息,改写sql模板,替换逻辑表为物理表,对同一个分片,同一个分片表的sql分组,得到对应的参数值组,然后就是使用jdbc api进行预处理的批量插入.批量插入的sql一般会生成两种

对于values(xxx,xxx,xxx),(xxx,xxx,xxx)语法中都是字面量的时候,Mycat2会把它们参数化,得到values(?,?,?)和两组参数值.

而对于values中使用表达式的情况,比如values(1+1,1),(2,2)这个情况,参数化结果是values(?+?,?),(?,?)和一组参数值,

当生成执行器,参数应用的时候,会先应用参数值然后把values(?+?,?),(?,?)拆分成两组values(?+?,?)与(?,?),计算分片节点,然后再使用jdbc插入

3.如果MycatInsert仅仅涉及一个分片的时候,同时Mycat2当前的事务模式是proxy的,就会把sql模板与参数值结合生成最终的sql,然后使用proxy api执行.

4.对于涉及到全局序列号的生成,mycat会修改sql模板,如果插入的字段没有自增序列,则sql模板补充自增字段.序列号的值作为预处理语句的参数值.



##### MycatUpdate

1.对于普通表,全局表的update,delete,insert,使用该算子表达

2.对于分片表的update,delete也是使用该算子表达

3.该算子把sql参数化,得到sql模板,和待求值的分片信息,当生成执行器的时候,应用参数值,得到对应的分片范围,然后使用物理表替换sql模板中的逻辑表,然后使用jdbc预处理语句进行执行

4.与MycatInsert相同,如果处于proxy模式,则尝试使用proxy模式执行

5.该算子与MycatInsert的不同点是MycatUpdate的生成的变化点在于分片信息,具体是根据分片信息修改sql的表名,而其他部分无需多加处理,而MycatInsert则复杂得多.

6.与MycatInsert都是针对分库分表设计的算子,最终执行的形态都是sql



##### MycatTableModify

1.该算子暂未启用,用于表示对查询结果的行数据修改应用到涉及的物理表上的运算

2.涉及的物理表要求配置主键信息

3.该算子也准备用于表达内置表的修改



##### MycatView

1,MycatView划分Mycat运算的算子与数据库运算的算子(物理节点算子)

2.MycatView使用逻辑表的逻辑关系表达式与分片具体值无关的数据分布信息表示,要么是全表信息,要么是参数化的分布信息,要么是具体的分布信息.对于参数化的分布信息,在真正求值的时候,也就是应用参数值的时候才产生具体的分布信息.如果无法找不到有效的分片条件的时候,就会使用全表信息,全表信息也是具体的分布信息.特殊地对于物理表,

分布信息肯定是具体的,分片信息就是一个分片的,而且关系表达式就是物理表相关的,不包含逻辑表.

3.与之相对的是MycatTransientSQLTableScan

4.当MycatView之下涉及多个分片的时候,生成执行器可能就带有并行拉取这些分片数据的特性,而无需显示的Gather算子指定并行



##### MycatTransientSQLTableScan

1.使用sql与分片目标表达的数据源算子,与MycatView不同的是,MycatTransientSQLTableScan就是具体的sql字符串

2.没有关系表达式,而且就是一对一的,一个分片目标与一个sql.而MycatView之下可能还涉及多个分片目标,但是只有一个关系表达式



##### 建立临时表

​		有一些要求输入的数据源支持重复计算（迭代，rewind）的执行器，Mycat2在不支持重复迭代的**输入源**的执行器会加上一个保存行数据的包装器，这个包装器会保存输入的执行器的数据，这样就可以得到支持重复迭代的执行器。



#### 一代与二代的配置不同

​		在第一代查询引擎中,全局表配置是任意的,而没有关于普通表的配置.这种设计的原因是配置一个单分片信息的全局表就具备普通表的功能.当分片表join全局表的时候,union的上推和分片sql的计算能把全局表与join的物理表join,然后合拼,并把相同分片节点的关系表达式变成sql.

​		而在第二代查询引擎中,使用自定义的sql生成规则,对于全局表的语义,在join总是两个子关系表达式式,多个join的时候总是以二叉树的形态存在的时候,涉及全局表的join总是能下推的,如果全局表的schema与table名字不一样,那么还需要计算对应的物理表是哪个.这样略显复杂,冗余,所以规定在第二代查询引擎,全局表总是要存在所有分片,而且库名表名都一致,包括逻辑表.而对于普通表,如果采用MycatView的方法,即使是同一个分片的下推也是要计算对应的物理表。



#### 全局表,普通表,分片表

​		在第二代查询引擎中,要求全局表,普通表的物理库名字,物理表名字分别与逻辑库名字,逻辑表名字一致.

这样的设计的好处是,mycat对于非用户关心的sql,比如show语句,无需进行sql改写表名,直接发到对应的物数据源上就可以了.更进一步的设计是指定一个物理数据源专门处理这些sql.而第一代查询引擎涉及到了sql改写,稍微复杂.

​		另外一个方面,全局表要求每个数据源上都有对应的物理表,而不像1代中,可以任意配置.这是为了与ER表下推全局表做简化处理,如果简化下推全局表的检查过程.对于普通表,只需检查sql的涉及的表信息,只有普通表的时候,直接把原sql发到数据源上即可,更一步的设计是固定一个数据源,对于普通表,或者没有配置的表的sql,直接发到此节点.普通表与没有配置的表的区别是前者显式制定了他们会参与分片路由,而后者,一般来说,只是进行读写分离处理.对于分片表,分析出是sql中只有一个表,而且分析得出的分片算法。

​		对于分片表,物理库与物理表的名字一般具有规律,有规律的名字容易与分片规则结合,生成物理表信息.无论在第一代查询引擎与第二代查询引擎中,都有明确指定配置分片信息设计倾向.主要原因是有些用户分库分表,有些是先分库,此时他们的物理表,名字是相同的,有些用户是先分表,他们的名字并不相同,还有一些用户一开始就是分库分表,然后选择分库来扩展性能.加上有些用户的系统是已经有分片的设计,后来再使用Mycat做分库分表,有自己的表名规则.情况比较复杂,所以mycat在配置分片表,一般要求配置所有的存储节点。



#### 关于分片算法

​		1.分片算法是根据分片条件计算出分片信息的对象,而分片信息一般就是指向数据源,物理库,物理表信息的对象,Mycat会使用此信息对sql进行改写,一般来说一个分片信息只涉及一个表信息,结合上述的ER表下推,就可以理解这个涉及多表下推的复杂。

​		2.配置文件中dataNodes是供分片算法提供全表扫描的信息,而分片算法可以根据实际情况,比如根据分片值得出物理库,物理表名.当在条件中找不到分片值的时候,使用全表扫描.Mycat2的分片算法的接口接收完整的条件表达式,用户可以根据实际情况写分片算法实现计算分片。



#### 关于DataNode

​		Mycat2总体设计上模糊了在1.6中dataNode的概念,并不明确.在1.6中,dataNode数据源和物理库名为固定信息,而要求表名在路由中改写(Mycat先去掉sql中的库名.分库不改写表名,分表改写表名),往大一点的方向来说,这个dataNode强调了节点这个概念,而固定物理库名实际上是路由的改写要求,.而在2.0中一个dataNode在配置中体现就是表分片(以后考虑配置中把dataNode更名，减少混乱).即其中一个分表,无论他是在一个数据源上还是在不同的物理库.在Mycat2的早期sql生成机制中,如果只涉及一个数据源的sql,就使用union all合拼sql,也就是说,无论跨库还是跨表,总能把查询逻辑表的sql翻译成查询多个物理表的一条sql.但是由于mysql的执行机制问题,性能并没有显著提高.所以Mycat2显著的设计是默认对于涉及多个物理表的查询,优先并行拉取多个数据源的数据,而使用union all合拼同一个数据源的结果集则是次要考虑。























