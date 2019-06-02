

# parsing state machine of mysql packet 

author:junwen 

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

When we talk about high performance proxy,we think of  keywords such as stream,forward etc.But we rarely find a mysql proxy is to send data that do not need to receive a complete mysql packet-based as unit to send to the client. For example,although mysql  client provide a way to read row data once time,if a row also large it still a problem as same as proxy.So  proxy  maybe provide a way to passthougth data in incomplete packet.

Firstly,a proxy supports connection reuse should judge whether  response  is end and whether connnection is free.Server status marks cursor,more result set and transcation status in OK packet ,EOF packet and COM_STMT_PREPARE_OK .They should be completed best because they help resolve the problem.And then,  Error packet have meaningful error message and it is also the end of response.Column count packet is also a key to count down the number of column def packet. Althouth proxy only need the server status but because they are small length,it simplified read packet completely.

Second,proxy must better know the type of each packet help judge  OK packet,EOF Packet because the header of payload is conflicting(Generally,the type of packet depending on the first byte of payload). The type of packet is context-dependent.The parse of packet  need context-driven.

Request packet is a optional reading complete packet.Because almost all payload require a proxy to analyze the them and then forward  them.Particularly,if a request payload is large like load data infile,packet Incomplete forwarding is meaningful. But Proxy can simplified read packet completely.

Based on the above analysis,I know that which packet needs to be read completely, and which packet can be forwarded without completely.I designed a state machine as follows.

```
reveive data...
while(response is not end){
	if type of payload need completely{
			read payload completely
			set current type of payload
			set whether response is end 
			converting data to other object or formats etc.
	}else {
			read packet partly
			set current type of payload when the end of payload
			set whether response is end when the payload in packet has reveived 					completely
			send part packet to front or store it etc.
	}
}
```

In mysql packet,a payload is sent through one or  splitting packets(multi).In most cases,the length of payload is far less than 0xffffff-1,so one payload is in one packet.Of course, we can not deal with splitting packet because it may appear in load data infile or large row but we can not.Mysql proxy can parse length packet and payload head by reading the first packet 5 bytes of payload so that  proxy can analysis the type of payload According to the context.For the rest of data of packet,proxy may reveive packet completely or record the remains bytes and then process the reveived data(send it to client for proxy),payload For the  splitting packets,proxy should judge the last packet of payload so that  judge the end of payload.



spliting packet

```
read data...
if the length of data >=4
	read the length of payload
		if the length of payload  < 0xffffff-1
        			not spliting packet
        if the length of payload  = 0xffffff-1
        			spliting packet
```



judge payload end

```
if the last packet is spliting and current packet is not
	payload spliting  end
if the last packet is spliting and current packet is spliting
	payload not end
if the last packet is not spliting and current packet is spliting
	payload not end(payload spliting first packet)
if the last packet is not spliting and current packet is not spliting
	payload end(payload not spliting first packet)
```



judge the type of payload

```
if packet is the first packet of payload
	read the first byte of payload to judge type of payload
if packet is not the first packet of packet
		read the first byte of payload,
		the length of payload(resolve the conflicting in the end of row)
		type of last payload(packet)(resolve the conflicting)
		maybe column count 
	
```



read packet partly

```
read data...
if remains bytes > 0
	remains bytes = remains bytes - the length of reveive payload
if the length of packet is not complete.
	remains bytes = the length of payload - the length of reveive payload
```



read payload completely

```
until the end packet data of payload is complete.
```

