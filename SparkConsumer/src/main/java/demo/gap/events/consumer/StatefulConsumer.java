package demo.gap.events.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;

import demo.gap.events.dto.CartEvent;

public class StatefulConsumer {
	public static void main(String[] args) throws Exception {
		// JavaStreamingContextFactory fact = new JavaStreamingContextFactory()
		// {
		//
		// };

		ObjectMapper mapper = new ObjectMapper();

		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaRecoverableNetworkWordCount");
		// Create the context with a 1 second batch size
		JavaStreamingContext streamingContext = new JavaStreamingContext(
				sparkConf, Durations.seconds(10));
		streamingContext.checkpoint("/tmp/chkpoint");

		Map<String, String> kafkaParams = new HashMap<>();

		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("mytopic");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(streamingContext, String.class,
						String.class, StringDecoder.class, StringDecoder.class,

						kafkaParams, topics);

		
		
	    JavaDStream<CartEvent> events = directKafkaStream.flatMap(new FlatMapFunction<Tuple2<String, String>, CartEvent>() {

	

			@Override
			public Iterator<CartEvent> call(Tuple2<String, String> t)
					throws Exception {
				CartEvent ce = mapper.readValue(t._2, CartEvent.class);
				
				List<CartEvent> a = new ArrayList<CartEvent>();
				a.add(ce);
				return a.iterator();
			}

	    });
		
		

		// id1,Cust | id2, Cust| id1, cust
		// first we group these by (id => id1, {cust, cust}) | (id2=>{cust})
		// do we then need to extract skus and form another pair rdd like:
		// (id1_sku1,Cust)?
		// create JavaMapWithStateDStream and store customer id as key and list
		// of items...
		// aggregate quantities if you see 2 add events with the same sku
		// RDD=> CustId, List

		// do i need to call directKafkaStream.map() to convert

		JavaPairDStream<String, CartEvent> newPairStream = events
				.mapToPair ( new PairFunction< CartEvent, String,  CartEvent>() {

					@Override
					public Tuple2<String, CartEvent> call(
							CartEvent t) throws Exception {

						// this will return the pairs as
						// custid_sku, CartEvent

						// after that we will reduce where we will
						// ADD if CartEvent.event == ADD and remove if
						// CartEvent.event == DELETE


						return new Tuple2<String, CartEvent>(t.getCustomer().getCustomerId(), t);
					}
				});

		// do i need the following fn below or just call map with state
//		JavaPairDStream<String, CartEvent> d = newPairStream
//				.reduceByKey(new Function2<CartEvent, CartEvent, CartEvent>() {
//
//					@Override
//					public CartEvent call(CartEvent v1, CartEvent v2)
//							throws Exception {
//
//						// if v1.event == ADD and v2.event == ADD, then add
//						// quantity
//
//						// if v1.event==ADD and v2.event == DELETE, subtract
//						// quantity.. if 0, return delete maybe?
//
//						// what if v1.event==DELETE and v2.event == DELETE?
//
//						return null;
//					}
//				});

		
	    Function3<String, CartEvent, State<CartEvent>, Tuple2<String, CartEvent>> mappingFunc =
	new Function3<String, CartEvent, State<CartEvent>, Tuple2<String, CartEvent>>() {
	              @Override
	              public Tuple2<String, CartEvent> call(String word, CartEvent one,
	                  State<CartEvent> state) {
//	                int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
//	                Tuple2<String, Integer> output = new Tuple2<>(word, sum);
//	                state.update(sum);
	                //return output;
						return new Tuple2<String, CartEvent>("newKey", new CartEvent());
	              }
	            };
	            

//    // DStream made of get cumulative counts that get updated in every batch
//    JavaMapWithStateDStream<String, CartEvent, CartEvent, Tuple2<String, CartEvent>> stateDstream =
//    		newPairStream.mapWithState(StateSpec.function(new Function3<String, Optional<CartEvent>, State<CartEvent>, Tuple2<String, CartEvent>>() {
//
//				@Override
//				public Tuple2<String, CartEvent> call(String v1,
//						Optional<CartEvent> v2, State<CartEvent> v3)
//						throws Exception {
//					// TODO Auto-generated method stub
//					return new Tuple2<String, CartEvent>("newKey", new CartEvent());
//				}
//    			 
//    		
//    		}));

		streamingContext.start();
		streamingContext.awaitTermination();

	}
}
