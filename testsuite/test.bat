SET curPath= %cd%
copy /Y  %curPath%\target\*.jar  %curPath%\target\lib
java -cp target/lib/* io.mycat.testsuite.tools.Testsuite src/main/resources/example.iq src/main/resources/example.iq
