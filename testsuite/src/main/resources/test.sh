#!/bin/bash
echo "----------------------------Mycat2 testsuite----------------------------"
jar_name="$(find . -name 'mycat2-testsuite-*.jar')"
java -jar $jar_name --factory "io.mycat.testsuite.TestConnectionFactory" --command-handler "io.mycat.testsuite.TestCommandHandler"  "target/example.iq" "src/main/resources/example.iq"