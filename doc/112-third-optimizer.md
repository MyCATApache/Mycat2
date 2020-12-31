Mycat2第三代查询器

author:chenjunwen 2020-12-31



[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).



  2020-8-20,完成了第二代查询优化器与执行器后.因为对用户的环境上执行一些日期函数不支持,印象特别深刻,所以马上对新的查询引擎(与其说是优化器,不如说是基于Calcite改造得出整个查询引擎).情况并不乐观,Calcite对日期函数支持程度比较低,并不完整兼容MySQL的日期函数的例子,整体并不乐观.于是设计一些方案去改善这个情况.



改造方案1:

  基于字符串作为传参,然后使用自己编写的函数实现Calcite不支持的函数,遇上难以实现的函数,则使用jdbc拼接sql字符串,传递到MySQL运算返回结果.这方案是成功的,暂时缓解了问题,但是没有彻底解决问题.对于语法解析与序列化的问题没有解决.Calcite立足于标准SQL,不支持变参函数,所以Mycat2只能暂时枚举用户用到的函数参数来实现暂时支持.



改造方案2:

  修修补补是没有前途的.由于在第二代查询引擎中已经实现了自研的执行器.所以完全可以在生成执行器的时候把没有找到对应函数的标量表达式,它们可以在Mycat的函数列表里面找到,以此生成函数实现.然后进一步把Calcite的validate进行少量修改,以此支持变参函数.该方案没有解决Calcite的时间单位缺失的问题,也就说,根据MySQL官网的日期函数例子是不能跑过测试.



改造方案3:

为了支持日期函数,我们把整个Calcite查询流程进行改造,主要工作如下

文本直接量支持缺失时间单位

改造Calcite的时间类型(涉及类型系统,执行器,解析器,类型转换函数,以及SQL解析,以及相关函数的转换成SQL)

timestamp=>java.time.LocalDateTime

time=>java.time.Duration

Date=>java.time.LocalDate



特别地,为了表示时间间隔

使用 java.time.Period,java.time.Duration来表示

这一次改造之后,终于对Mycat2的查询器越来越有信心了



改造方案4:

  为了使整个Calcite查询器能完整支持MySQL语法,使用Alibaba Druid转换为Calcite SqlNode的方案.但是有些语法元素是没有办法把MySQL的语法元素与Calcite的SqlNode对应.但是我又想得到一个基于Calcite优化器的MySQL方言的查询引擎.没有办法,还得进一步改造,对于Session级别与Global的级别的变量支持,以及for update语法的支持.于是设计特殊函数把session/global变量访问转换为函数,把for update记录在优化器上下文中,而在关系表达式转换为SQL时候再补上.

**这一次改造后,成功直接使用Calcite查询器就可以支持SQL控制台客户端,图形化客户端的sql,而无需路由部分.加以Mycat2本来就把所有系统表,视为自身的逻辑表的一部分,即使所有MySQL涉及系统表的SQL经过Calcite或者不涉及表的sql(比如获取系统变量),Calcite都能正确分析,这个为后来完整实现MySQL系统表打下了基础.**



  在这一次改造后,我们得到了一个函数家族,因为网上现有的库也没有完整实现Java版的MySQL函数,所以我一个一个把它们实现.最终我获得了一个java版的MySQL函数工具类(欢迎大家使用),

它支持以Java的不可变对象

java.lang.Byte

java.lang.Short

java.lang.Boolean

java.lang.Integer

java.lang.Long

java.math.BigDecimal

java.lang.Float

java.lang.Double

java.time.LocalDate

java.time.Duration

java.lang.String

org.apache.calcite.avatica.util.ByteString

org.apache.calcite.avatica.util.TimeUnitRange

为支持,整体与PolarDB-X 的函数支持程度一致(https://help.aliyun.com/document_detail/71269.html)

直到Mycat2改造完成后才发现PolarDB-X也没有支持如此细致的时间类型.

此处不再列出到底是哪些函数,大约100多个.



改造方案5:

最后,我实现了上述的不可变对象之间的类型转换,彻底改造了Calcite的执行器的类型转换表达式.

