# GPattern模式匹配

author:junwen 2019-10-2

qq:294712221

<https://github.com/junwen12221/GPattern>

## 快速入门

```java
GPatternBuilder patternBuilder = new GPatternBuilder(0);
int id = patternBuilder.addRule("SELECT id FROM {table} LIMIT 1;");
GPattern gPattern = patternBuilder.createGroupPattern();
GPatternMatcher matcher = gPattern.matcher("SELECT id FROM travelrecord LIMIT 1;");
Assert.assertTrue(matcher.acceptAll());
Assert.assertEquals(0, id);
Assert.assertEquals("travelrecord", gPattern.toContextMap(matcher).get("table"));
```



## 词法分析器

#### 目标

按照以下词法规则把UTF8或者ASCII的字节数组分组成多个词法单元



#### 空格

空格分隔词法单元

```
 
\t
\f
```



#### 注释

注释用来在源码中增加提示、笔记、建议、警告等信息.这些信息在词法分析中会被忽略.

##### **单行注释（**single-line comment）

```
-- 单行注释
// 单行注释
# 单行注释
```

##### **多行注释（**multiple-line comment）

```
/* 多行注释 */
/* 

多行注释 

*/
```



### 词法单元(Token)

#### 直接量

直接量仅支持ASCII字符

##### 字符串直接量

```
'id'
"id"
```

**转义字符串直接量**

```
`id`
```



#### 标识符

标识模式中的ASCII直接量字符序列。

##### 单一字符

以空格分隔的长度是1的ASCII字符

##### 字符序列

以空格分隔的包含字母或数字或下划线（“_”）或美元符号（“$”）的字符序列(长度大于1)的ASCII字符



#### 大小写

默认忽略大小写,把大小转换成小写,可以关闭此特性



## 模式匹配

### 名称捕获

捕获多个或者一个词法单元,把它们在字节数组中的开始位置,结束位置保存,可以根据名称获取它们在字节数组中的范围

{name},其中name必须是ASCII字符

待匹配字节数组允许UTF8字符



##### 一个词法单元捕获

模式：

```
SELECT id FROM {table} LIMIT 1;
```

待匹配字符串

```
SELECT id FROM travelrecord LIMIT 1;
```

可以根据name获得travelrecord



##### 多个词法单元捕获

待匹配字符串

```
SELECT id FROM travelrecord ，travelrecord2 LIMIT 1;
```

可以根据name获得

travelrecord ，travelrecord2



##### 直接量优先匹配

模式：

```
SELECT id FROM {table} LIMIT 1;// 模式1 
SELECT id FROM {table} LIMIT {n};// 模式2
```

待匹配字符串

```
SELECT id FROM travelrecord LIMIT 1;
```

模式1匹配



## 模式ID

```java
GPatternBuilder patternBuilder = new GPatternBuilder(0);
int id = patternBuilder.addRule("{any} FROM travelrecord  {any2}");//一个模式对应一个id
GPattern gPattern = patternBuilder.createGroupPattern();
GPatternMatcher matcher = gPattern.matcher("SELECT id FROM travelrecord LIMIT 1");
Assert.assertTrue(matcher.acceptAll());
Assert.assertEquals(0, id);
Assert.assertEquals(id, matcher.id());//模式匹配成功后,可以根据匹配器获取模式对应的id
Map<String, String> map = gPattern.toContextMap(matcher);
Assert.assertEquals("SELECT id", map.get("any"));
Assert.assertEquals("LIMIT 1", map.get("any2"));
```

## License

GPLv3

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)

This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------





