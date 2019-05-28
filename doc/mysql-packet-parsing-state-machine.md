# parsing state machine of mysql packet 

When we talk about high performance proxy,we think of  keywords such as stream,forward etc.But we rarely find a mysql proxy is to send data that do not need to receive a complete mysql packet-based as unit to send to the client. Thougth mysql  client provide a way to read row data once time,if a row also large.So  proxy provide a way to passthougth data in incomplete packet.

Firstly,some payload should be complete best because they are meta data.

server status is very important infomation that marked cursor,more result set,transcation staus in OK packet ,EOF packet and COM_STMT_PREPARE_OK.Althouth proxy only need these infomation so  



