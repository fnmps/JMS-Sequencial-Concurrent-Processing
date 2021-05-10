# JMS-Sequencial-Concurrent-Processing
An implementation of a JMS consumer that processes message sequentially if the messages have the same key but concurrently if they have different keys.

##Description of Solution

There are two main classes to reading messages concurrently while keeping the order of message per key:
 
*SequenceManager
*AbstractKeySequenceMessageListener

The SequenceManager is responsible from reading the queues sequentially and assigning a different JMS session (from a pool) per message.
A different session is given per message so that when a message is acknowledged, it does not acknowledge any other message that has already been read.

The AbstractKeySequenceMessageListener is responsible of keeping track of order of the messages with the same key.

###Algorithm

Considering an MQueue with the following messages (and respective keys):
<p align="center">
	<img src="https://github.com/fnmps/JMS-Sequencial-Concurrent-Processing/blob/main/README%20Resources/img1.png?raw=true" width="100">
</p>

The SequenceManager will read each of the messages sequentially in a separate session, extract the key from the message and delegate them to the AbstractKeySequenceMessageListener.
The Listener will then check if an internal queue already exists for the key of the message. If there is, then the message is added at the end of that queue. If not, then a new internal queue is created and the message is added to that queue.

<br />
<br />
<p align="center">
	<figure class="image">
		<figcaption>Step 1</figcaption>
	  	<img src="https://github.com/fnmps/JMS-Sequencial-Concurrent-Processing/blob/main/README%20Resources/img2.png?raw=true" width="400">
	</figure>
</p>
<br />
<br />

<p align="center">
	<figure class="image">
		<figcaption>Step 2</figcaption>
	  	<img src="https://github.com/fnmps/JMS-Sequencial-Concurrent-Processing/blob/main/README%20Resources/img3.png?raw=true" width="400">
	</figure>
</p>

<br />
<br />
<p align="center">
	<figure class="image">
		<figcaption>Step N</figcaption>
	  	<img src="https://github.com/fnmps/JMS-Sequencial-Concurrent-Processing/blob/main/README%20Resources/img4.png?raw=true" width="400">
	</figure>
</p>
<br />
<br />
When a message is added to the respective internal queue, the listener will create a new thread that will wait for the current message to be the first in the internal queue and perform the task specified on the doTask method of implemented on the Listener.
<br />
<br />
<p align="center">
	<img src="https://github.com/fnmps/JMS-Sequencial-Concurrent-Processing/blob/main/README%20Resources/img5.png?raw=true" width="400">
</p>
<br />
<br />

Once the task for the first element of the internal queue is completed the message will be acknowledge, the JMS session committed and remove the element from the internal queue.
If the queue is empty, meaning no other message with the same key has been received since the completion of the task, then the internal queue deleted to save memory.
<br />
<br />
<p align="center">
	<img src="https://github.com/fnmps/JMS-Sequencial-Concurrent-Processing/blob/main/README%20Resources/img6.png?raw=true" width="600">
</p>

## Usage

To use the SequenceManager the following implementation needs to be done:

1\. Implement a class extending AbstractKeySequenceMessageListener

```java
public class MyMessageListener extends AbstractKeySequenceMessageListener {

	public MyMessageListener(int maxNbThreads) {
		super(maxNbThreads);
	}

	@Override
	public void doTask(Message message) {
		// perform the task for the message
	}

	@Override
	public void onMessageError(Message message, Exception e) {
		// error handling code
	}

}
```
2\. Implement a class implementing MessageKeyExtractor

```java
public class MyMessageKeyExtractor implements MessageKeyExtractor{

	@Override
	public String extractKey(Message message) throws JMSException {
		String myKey = "";
		//Extract key from message
		return myKey;
	}

}
```

3\. Create a JMS Connection Factory

```java
MQConnectionFactory connectionFactory = new MQConnectionFactory();
connectionFactory.setHostName(MQ_HOST_NAME);
connectionFactory.setPort(MQ_PORT);
connectionFactory.setQueueManager(MQ_MANAGER);
connectionFactory.setChannel(MQ_CHANNEL);
connectionFactory.setTransportType(MQ_TRANSPORT_TYPE);
```

4\. Instantiate the Sequence Manager

```java
new SequenceManager(QUEUE_NAME, connectionFactory, new MyMessageListener(100), new MyMessageKeyExtractor()))
```
