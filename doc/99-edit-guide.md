# 文档编辑指南

author:junwen 2019-6-2

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

1. markdown编辑软件推荐使用Typora

2. 生成文档使用pandoc

   ```bash
   pandoc -f markdown -o mycat2.0-guide.html  title.txt 00-mycat-readme.md 01-mycat-proxy.md  02-mycat-user.md  03-mycat-replica.md 04-mycat-schema.md 05-mycat-dynamic-annotation.md 06-mycat-function.md 07-mycat-heart.md 08-mycat-static-annotation.md  09-mycat-plug.md
    100-mysql-packet-parsing-state-machine.md ..........
   ```

