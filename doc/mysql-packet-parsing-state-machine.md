# parsing state machine of mysql packet 

When we talk about high performance proxy,we think of  keywords such as stream,forward etc.But we rarely find a mysql proxy is to send data that do not need to receive a complete mysql packet-based as unit to send to the client. For example,although mysql  client provide a way to read row data once time,if a row also large it still a problem as same as proxy.So  proxy  maybe provide a way to passthougth data in incomplete packet.

Firstly,some payload should be complete best because they are meta data.server status is very important infomation that marked cursor,more result set,transcation staus in OK packet ,EOF packet and COM_STMT_PREPARE_OK.Althouth proxy only need the server status but beacuse they are small length,it simplified read packet fully.

Second,proxy must know what the  request or response  ends after what type of packet or mark on the packet.When a mysql server sends a Error packet,the response ends.And the last packet of result set is with a head 0xfe without more result set mark in server status,the response ends.Normally, a command response is OK packet or Error packet or Eof packet.

Column count is also a key to count down the number of column def packet.so proxy must read it fully.

Request packet is a optional reading fully packet.Because almost all payload require a proxy to analyze the them and then forward  them.Particularly,if a request payload is large like load data infile,packet Incomplete forwarding is meaningful. But Proxy can simplified read packet fully.

Based on the above analysis,we know that which packet needs to be read completely, and which packet can be forwarded only by judging its packet type.







