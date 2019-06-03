# mycat 2.0-package

author:junwen 2019-6-3

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

## maven

mycat maven module

```bash
mvn package
```

## fat jar

```bash
mycat2-${project.version}-${timestamp}-single.jar
```

```bash
java -Dfile.encoding=UTF-8 -DMYCAT_HOME=d:/xxx -jar mycat2-${project.version}-${timestamp}-single.jar
```

## tar.gz

decompression

```bash
mycat2-${project.version}-${timestamp}.tar.gz
```

Set the system environment variable MYCAT_HOME and configure the path of the folder

```bash
MYCAT_HOME = xxx
```

Reference Java Service Wrapper

------

