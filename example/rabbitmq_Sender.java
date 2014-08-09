
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
public class send {

  private final static String QUEUE_NAME = "QueueName";

  public static void main(String[] argv) throws java.io.IOException
  {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    String message = "Write message here";
 while (!Thread.currentThread ().isInterrupted ()) 
{
    channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
    System.out.println(" [x] Sent '" + message + "'");
}
    channel.close();
    connection.close();
  }
}
