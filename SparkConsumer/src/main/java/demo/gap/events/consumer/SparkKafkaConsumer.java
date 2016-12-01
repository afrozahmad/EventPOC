package demo.gap.events.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.apache.spark.streaming.kafka010.*;

public class SparkKafkaConsumer {

	private static final String ZK_QUORUM = "localhost:2181";

	public static void main(String[] args) throws InterruptedException {

		Set<String> topics = Collections.singleton("mytopic");

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"CartEventListener");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf,
				Durations.seconds(10));

		Map<String, String> kafkaParams = new HashMap<>();

		kafkaParams.put("metadata.broker.list", "localhost:9092");


		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(streamingContext, String.class,
						String.class, StringDecoder.class, StringDecoder.class,

						kafkaParams, topics);

		
		
		
		directKafkaStream.foreachRDD(rdd -> {
			
			JavaPairRDD<String, String> rddTyped = (JavaPairRDD<String, String>)rdd;
		
			System.out.println("--- New RDD with " + rdd.partitions().size()
					+ " partitions and " + rdd.count() + " records");
			rdd.foreach(record ->{ 
				System.out.println(record.getClass());
				System.out.println(record._1);
				System.out.println(record._2);
			});
		});

		streamingContext.start();
		streamingContext.awaitTermination();	
		
	}
}
