Concept
=======

A reliable multicast protocol written in Java.  

How it works
============
A given PandaAdapter can subscribe to a topic, send a topic, or do both.  Each adapter that wants to send 
data sends it with a sequence number that is maintained by that sender.  If a receiver is interested in a 
particular topic, it maintains the sender's sequence number to detect a gap.  If there are mutliple sending 
nodes on a given topic, the receiver will detect each of the senders' sequener number separately.  If a gap
is detected, the receiver will establish a tcp connection to the sender requesting the missed data.  This
connection wil be terminated once the sender has completed its response.

Details
=======
* Sender determines how many messages to queue in case any receiver misses data
* Sender can choose not to queue any data, in which case, the receiver would not attempt a gap request.
* Receiver will wait 20 packets or 2 seconds, whichever comes first, for sending a gap request.  This is to handle out of order packets.
* Receiver will establish a connection to the sender for each request it needs.
* Receiver will only send one request at a time per channel per sender.


