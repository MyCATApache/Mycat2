# mycat2



How to build source code into a jar?

```
cd mycat2
compile assembly:single
```

or you can

set the Working directory to mycat2  and

set command line to compile assembly:single

in your run/debug configuration.



how to run/debug?

path to the configuration file(resources) as MYCAT_HOME added to VM options.

```
java -Dfile.encoding=UTF-8 -DMYCAT_HOME=D:\xxxxxxx -jar mycat2-0.1.jar 
```



## License

Apache-2.0 license