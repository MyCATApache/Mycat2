#!/bin/sh

#check JAVA_HOME & java
noJavaHome=false
if [ -z "$JAVA_HOME" ] ; then
    noJavaHome=true
fi
if [ ! -e "$JAVA_HOME/bin/java" ] ; then
    noJavaHome=true
fi
if $noJavaHome ; then
    echo
    echo "Error: JAVA_HOME environment variable is not set."
    echo
    exit 1
fi
#==============================================================================
#set JAVA_OPTS
#performance Options
#GC Log Options

#debug Options
#JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=8065,server=y,suspend=n"
#==============================================================================

#set HOME
CURR_DIR=`pwd`
cd `dirname "$0"`/..
MYCAT_HOME=`pwd`
cd $CURR_DIR
if [ -z "$MYCAT_HOME" ] ; then
    echo
    echo "Error: MYCAT_HOME environment variable is not defined correctly."
    echo
    exit 1
fi
#==============================================================================

#set CLASSPATH
MYCAT_CLASSPATH="$MYCAT_HOME/conf:$MYCAT_HOME/lib/classes"
for i in "$MYCAT_HOME"/lib/*.jar
do
    MYCAT_CLASSPATH="$MYCAT_CLASSPATH:$i"
done
#==============================================================================

#startup Server
RUN_CMD="\"$JAVA_HOME/bin/java\""
RUN_CMD="$RUN_CMD -DMYCAT_HOME=\"$MYCAT_HOME\""
RUN_CMD="$RUN_CMD -classpath \"$MYCAT_CLASSPATH\""
RUN_CMD="$RUN_CMD $JAVA_OPTS"
RUN_CMD="$RUN_CMD io.mycat.MycatCore  $@"
RUN_CMD="$RUN_CMD >> \"$MYCAT_HOME/logs/console.log\" 2>&1 &"
echo $RUN_CMD
eval $RUN_CMD
#==============================================================================
