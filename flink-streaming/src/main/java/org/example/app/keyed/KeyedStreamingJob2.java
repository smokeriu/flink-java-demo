/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.app.keyed;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 * @author liushengwei
 */
public class KeyedStreamingJob2 {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final Configuration conf = new Configuration();
		conf.setString("flink.partition-discovery.interval-millis","1");
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		env.setParallelism(2);
		env.getConfig().setAutoWatermarkInterval(1);


		// kafka config
		final Properties props = new Properties();
		props.put("bootstrap.servers", "cdh02:9092,cdh03:9092,cdh04:9092,cdh05:9092");
		props.setProperty("group.id", "Flink" + System.currentTimeMillis());
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("auto.commit.interval.ms", "1000");
		// add consumer
		final FlinkKafkaConsumer<ObjectNode> myConsumer = new FlinkKafkaConsumer<>("flink_msg_keyed", new JSONKeyValueDeserializationSchema(true), props);
		myConsumer.setStartFromEarliest();
		// TODO: 并行度研究

		// 提取时间戳
		final WatermarkStrategy<ObjectNode> watermarkStrategy = WatermarkStrategy
				.<ObjectNode>forBoundedOutOfOrderness(Duration.ofSeconds(2))
				.withTimestampAssigner((SerializableTimestampAssigner<ObjectNode>) (element, recordTimestamp) -> element.get("value").get("log_time").longValue());

		myConsumer.assignTimestampsAndWatermarks(watermarkStrategy);


		// add source
		final DataStreamSource<ObjectNode> input = env.addSource(myConsumer);


		//process
		final SingleOutputStreamOperator<Tuple4<Integer, Long, Long, String>> baseInput = input.map(new MapFunction<ObjectNode, Tuple4<Integer, Long, Long, String>>() {
			@Override
			public Tuple4<Integer, Long, Long, String> map(ObjectNode value) throws Exception {
				final JsonNode recordValue = value.get("value");
				final int id = recordValue.get("id").asInt();
				final long logTime = recordValue.get("log_time").asLong();
				final long dinTime = recordValue.get("din_time").asLong();
				final String info = recordValue.get("info1").asText();
				return Tuple4.of(id, logTime, dinTime, info);
			}
		});

		//


		// window
		final KeyedStream<Tuple4<Integer, Long, Long, String>, Integer> keyedStream = baseInput.keyBy(((KeySelector<Tuple4<Integer, Long, Long, String>, Integer>) value -> value.f0));

		final SingleOutputStreamOperator<String> out = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(4)))
				.allowedLateness(Time.seconds(100))
				.process(new ProcessWindowFunction<Tuple4<Integer, Long, Long, String>, String, Integer, TimeWindow>() {

					// TODO: 笔记记录

					ReducingState<Integer> reduce;
					// 写在哪都可以
					final ValueStateDescriptor<Integer> cuntDescriptor = new ValueStateDescriptor<>("cunt", Types.INT);

					// open 方法不属于上下文
					@Override
					public void open(Configuration parameters) throws Exception {
						// 必须写在open方法里
						// 写在这里则是公有的state
						ReducingStateDescriptor<Integer> cuntDescriptor = new ReducingStateDescriptor<>("cunt", new ReduceFunction<Integer>() {
							@Override
							public Integer reduce(Integer value1, Integer value2) throws Exception {
								return value1 + value2;
							}
						}, Types.INT);
					}

					@Override
					public void process(Integer integer, Context context, Iterable<Tuple4<Integer, Long, Long, String>> elements, Collector<String> out) throws Exception {
						// 每个window独有的state
						final ValueState<Integer> state = context.windowState().getState(cuntDescriptor);
						final List<String> list = new ArrayList<>();
						// 初始化状态
						if(state.value() == null){
							state.update(0);
						}
						for (Tuple4<Integer, Long, Long, String> element : elements) {
							int i = state.value();
							state.update(i+1);
							list.add(element.f3);
						}
						final String res = String.join(",", list) + "---" + state.value();
						out.collect(res);
					}
				});

		input.print("input");
		out.print("out");
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
