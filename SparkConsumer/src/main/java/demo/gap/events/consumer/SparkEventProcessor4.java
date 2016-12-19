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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;

import demo.gap.events.dto.CartEvent;

public class SparkEventProcessor4 {

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

		JavaPairDStream<String, CartEvent> events = directKafkaStream
				.mapToPair(new PairFunction<Tuple2<String, String>, String, CartEvent>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, CartEvent> call(
							Tuple2<String, String> t) throws Exception {

						
						CartEvent ce = mapper.readValue(t._2, CartEvent.class);
						//System.out.println()
						
						String newKey = ce.getCustomer().getCustomerId();
						return new Tuple2<String, CartEvent >(newKey, ce);
					}

				});

		// Update the cumulative count function
		Function3<String, Optional<CartEvent>, State<CartEvent>, Tuple2<String, CartEvent>> mappingFunc = new Function3<String, Optional<CartEvent>, State<CartEvent>, Tuple2<String, CartEvent>>() {
			/**
		 *
		 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, CartEvent> call(String key,
					Optional<CartEvent> event, State<CartEvent> state) {
				// int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
				// Tuple2<String, Integer> output = new Tuple2<>(word, sum);
				// state.update(sum);
				// return output;
				if (state.exists()){

					
					CartEvent storedState = state.get();
					
					
							
					int totalQnty = 0, totalQntyState = 0;
					if (event.get().getEvent().equals("ADD")) {
						totalQnty = event.get().getItem().getQuantity();
					} else if (event.get().getEvent().equals("DELETE")) {
						totalQnty = -1 * event.get().getItem().getQuantity();
					}

					storedState.getItem().setQuantity(storedState.getItem().getQuantity() + totalQnty);
					System.out.println();
					state.update(storedState);
					return new Tuple2<String, CartEvent> (key, storedState);
					
					
					
				}
				else {
					state.update(event.get());
					return new Tuple2<String, CartEvent> (key, event.get());
				}
			
			}
		};
		// DStream made of get cumulative counts that get updated in every batch
		JavaMapWithStateDStream<String, CartEvent, CartEvent, Tuple2<String, CartEvent>> stateDstream = events
				.mapWithState(StateSpec.function(mappingFunc));

		stateDstream.print();

		stateDstream.foreachRDD(rdd -> {

			//JavaPairRDD<String, CartEvent> rddTyped = (JavaPairRDD<String, CartEvent>) rdd;

			System.out.println("--- New RDD with " + rdd.partitions().size()
					+ " partitions and " + rdd.count() + " records");
			rdd.foreach(record -> {

				if (record._2 == null) {
					System.out.println("record._2 is null");
				} else if (record._2.getEvent() == null) {
					System.out.println("record._2.getEvent is null");
				}
				System.out.print(record._1);
				System.out.println(" " + record._2.getItem().getQuantity());
			});
		});

		streamingContext.start();
		streamingContext.awaitTermination();
	}
}