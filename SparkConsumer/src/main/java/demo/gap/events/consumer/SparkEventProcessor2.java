/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo.gap.events.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
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

import scala.Tuple1;
import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;

import demo.gap.events.dto.CartEvent;
import demo.gap.events.dto.EventWrapper;

public class SparkEventProcessor2 {

	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName(
				"CartEventListener");

		JavaStreamingContext streamingContext = new JavaStreamingContext(
				sparkConf, Durations.seconds(10));
		streamingContext.checkpoint("/tmp/chkpoint3");

		Map<String, String> kafkaParams = new HashMap<>();

		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("mytopic3");
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(streamingContext, String.class,
						String.class, StringDecoder.class, StringDecoder.class,
						kafkaParams, topics);

		ObjectMapper mapper = new ObjectMapper();

		JavaDStream<EventWrapper> events = directKafkaStream
				.map(new Function<Tuple2<String, String>, EventWrapper>() {

					@Override
					public EventWrapper call(Tuple2<String, String> v1)
							throws Exception {

						EventWrapper ew = new EventWrapper();
						ew.setKey(v1._1);
						CartEvent ce = mapper.readValue(v1._2, CartEvent.class);
						ew.setEventId(ce.getEventId());
						
						ew.setEvent(ce);
						return ew;
					}

					
				
				
				});
		
		 JavaPairDStream< String, EventWrapper> wordsDstream = events.mapToPair(
				 new PairFunction<EventWrapper, String, EventWrapper>() {

					@Override
					public Tuple2<String, EventWrapper> call(EventWrapper t)
							throws Exception {
						
						
						return new Tuple2<String, EventWrapper>(t.getEvent().getCustomer().getCustomerId(), t);
					}
			        });


		// Update the cumulative count function
		Function3<String, EventWrapper, State<EventWrapper>, Tuple1<EventWrapper>> mappingFunc = new Function3<String, EventWrapper, State<EventWrapper>, Tuple1<EventWrapper>>() {
			/**
		 *
		 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple1<EventWrapper> call(String key, EventWrapper eventWrapper,
					State<EventWrapper> state) throws Exception {
				if (state.exists()){

					
					EventWrapper eventV1 = state.get();
					
					
							
					int totalQntyV1 = 0, totalQntyV2 = 0;
					
					
					return null;
					
					
				}
				else {
					
					return new Tuple1(eventWrapper);
				}
				
				
				
				
				
			}

			


		};
//		// DStream made of get cumulative counts that get updated in every batch
//		JavaMapWithStateDStream<String, CartEvent, CartEvent, Tuple2<String, CartEvent>> stateDstream = events
//				.mapWithState(StateSpec.function(mappingFunc));
//
//		stateDstream.print();
//
//		rd.foreachRDD(rdd -> {
//
//			JavaPairRDD<String, CartEvent> rddTyped = (JavaPairRDD<String, CartEvent>) rdd;
//
//			System.out.println("--- New RDD with " + rdd.partitions().size()
//					+ " partitions and " + rdd.count() + " records");
//			rdd.foreach(record -> {
//
//				if (record._2 == null) {
//					System.out.println("record._2 is null");
//				} else if (record._2.getEvent() == null) {
//					System.out.println("record._2.getEvent is null");
//				}
//				System.out.print(record._1);
//				System.out.println(" " + record._2.getItem().getQuantity());
//			});
//		});

		streamingContext.start();
		streamingContext.awaitTermination();
	}
}