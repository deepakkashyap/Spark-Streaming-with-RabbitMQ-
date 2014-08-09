
import java.io.IOException;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
public class spark 
{
	private spark()
	{  
		
	}
	 public static  JavaStreamingContext ssc;
	public static void main(String[] args) throws IOException 
	{
	      
	    
		    Duration batchInterval = new Duration(2000);
			
			ssc = new JavaStreamingContext("local", "Rabbit", batchInterval,System.getenv("SPARK_HOME"),
	            JavaStreamingContext.jarOfClass(spark.class));
			
			JavaDStream<String> CSR = ssc.receiverStream(new Rabbit());
                                     //here is rabbitmq stream as spark streaming
	  CSR.print();
	  
	    ssc.start();
	    ssc.awaitTermination();
}
}
