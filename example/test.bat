SET curPath= %cd%
copy /Y  %curPath%\target\*.jar  %curPath%\target\lib
java -cp target/lib/* io.mycat.example.booster.BoosterExample %curPath%/src/main/resources/io/mycat/example/booster %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.customrule.CustomRuleExample %curPath%/src/main/resources/io/mycat/example/customrule %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.customrulesegmentquery.CustomRuleSegExample %curPath%/src/main/resources/io/mycat/example/customrulesegmentquery %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.manager.ManagerExample %curPath%/src/main/resources/io/mycat/example/manager %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.pstmt.PstmtShardingExample %curPath%/src/main/resources/io/mycat/example/pstmt %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.readwriteseparation.ReadWriteSeparationExample %curPath%/src/main/resources/io/mycat/example/readwriteseparation %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.sharding.ShardingExample %curPath%/src/main/resources/io/mycat/example/sharding %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.shardinglocal.ShardingLocalExample %curPath%/src/main/resources/io/mycat/example/shardinglocal %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.shardinglocalfail.ShardingLocalFailExample %curPath%/src/main/resources/io/mycat/example/shardinglocalfail %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.shardingrw.ShardingRwExample %curPath%/src/main/resources/io/mycat/example/shardingrw %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.shardingxa.ShardingXAExample %curPath%/src/main/resources/io/mycat/example/shardingxa %curPath%/target test server exit
java -cp target/lib/* io.mycat.example.shardingxafail.ShardingXAFailExample %curPath%/src/main/resources/io/mycat/example/shardingxafail %curPath%/target test server exit


