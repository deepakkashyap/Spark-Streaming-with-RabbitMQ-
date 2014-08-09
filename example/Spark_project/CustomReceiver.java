import java.io.IOException;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;


public class Rabbit extends Receiver<String>
{
private final static String QUEUE_NAME = "QueueName";

private ConnectionFactory factory;
QueueingConsumer consumer;
String hostname="localhost";//define hostname for rabbitmq reciver
Connection connection;
Channel channel;

public Rabbit()
{
		super(StorageLevel.MEMORY_AND_DISK_2());
		
	}

@Override
public void onStart() {
	new Thread()  {
	      @Override public void run() {
	        
				receive();
		
	    }.start();
	  }
	


protected void receive() throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException
{
	
	  factory = new ConnectionFactory();
	    factory.setHost(hostname);
	    connection = factory.newConnection();
	  channel = connection.createChannel();

	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	   
	    
	 consumer = new QueueingConsumer(channel);
	    channel.basicConsume(QUEUE_NAME, true, consumer);
	  

	    while (!Thread.currentThread ().isInterrupted ()) 
	             {
	      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
	      String message = new String(delivery.getBody());
	      store(message);
	             }
}

@Override
public void onStop() {
	
}

}
