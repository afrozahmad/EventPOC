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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
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

public class SparkEventProcessor {

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

		JavaDStream<CartEvent> events = directKafkaStream
				.flatMap(new FlatMapFunction<Tuple2<String, String>, CartEvent>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<CartEvent> call(Tuple2<String, String> t)
							throws Exception {
						CartEvent ce = mapper.readValue(t._2, CartEvent.class);

						List<CartEvent> a = new ArrayList<CartEvent>();
						a.add(ce);
						return a.iterator();
					}

				});

		JavaPairDStream<String, CartEvent> d = events
				.mapToPair(new PairFunction<CartEvent, String, CartEvent>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, CartEvent> call(CartEvent t)
							throws Exception {

						// this will return the pairs as
						// custid_sku, CartEvent

						// after that we will reduce where we will
						// ADD if CartEvent.event == ADD and remove if
						// CartEvent.event == DELETE

						// CartEvent ce = mapper.readValue(t._2,
						// CartEvent.class);
						String newKey = t.getCustomer().getCustomerId() + "_"
								+ t.getItem().getSku();

						return new Tuple2<String, CartEvent>(newKey, t);
					}
				});

		// do i need the following fn below or just call map with state
		JavaPairDStream<String, CartEvent> rd = d
				.reduceByKey(new Function2<CartEvent, CartEvent, CartEvent>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public CartEvent call(CartEvent v1, CartEvent v2)
							throws Exception {
						if (v1 == null) {
							System.out.println("Event1 is null");
							return v1;
						}
						String eventV1 = v1.getEvent();
						if (v2 == null) {
							System.out.println("Event2 is null");
							return v1;
						}
						String eventV2 = v2.getEvent();
						int totalQntyV1 = 0, totalQntyV2 = 0;
						if (eventV1.equals("ADD")) {
							totalQntyV1 = v1.getItem().getQuantity();
						} else if (eventV1.equals("DELETE")) {
							totalQntyV1 = -1 * v1.getItem().getQuantity();
						}
						if (eventV2.equals("ADD")) {
							totalQntyV2 = v2.getItem().getQuantity();
						} else if (eventV2.equals("DELETE")) {
							totalQntyV2 = -1 * v2.getItem().getQuantity();
						}

						v1.getItem().setQuantity(totalQntyV1 + totalQntyV2);

						return v1;

						// if v1.event == ADD and v2.event == ADD, then add
						// quantity

						// if v1.event==ADD and v2.event == DELETE, subtract
						// quantity.. if 0, return delete maybe?

						// what if v1.event==DELETE and v2.event == DELETE?

						// temporarily return

					}
				});

		// Update the cumulative count function
		Function3<String, Optional<CartEvent>, State<CartEvent>, Tuple2<String, CartEvent>> mappingFunc = new Function3<String, Optional<CartEvent>, State<CartEvent>, Tuple2<String, CartEvent>>() {
			/**
		 *
		 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, CartEvent> call(String word,
					Optional<CartEvent> one, State<CartEvent> state) {
				// int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
				// Tuple2<String, Integer> output = new Tuple2<>(word, sum);
				// state.update(sum);
				// return output;
				return new Tuple2<String, CartEvent>("s", new CartEvent());
			}
		};
		// DStream made of get cumulative counts that get updated in every batch
		JavaMapWithStateDStream<String, CartEvent, CartEvent, Tuple2<String, CartEvent>> stateDstream = rd
				.mapWithState(StateSpec.function(mappingFunc));

		stateDstream.print();

		rd.foreachRDD(rdd -> {

			JavaPairRDD<String, CartEvent> rddTyped = (JavaPairRDD<String, CartEvent>) rdd;

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