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

package org.example;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.example.util.SensorReading;

import java.time.Duration;
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
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final Configuration conf = new Configuration();
		conf.setString("flink.partition-discovery.interval-millis","1");
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		env.setParallelism(1);
		env.getConfig().setAutoWatermarkInterval(1);


		// kafka config
		final Properties props = new Properties();
		props.put("bootstrap.servers", "cdh02:9092,cdh03:9092,cdh04:9092,cdh05:9092");
		props.setProperty("group.id", "Flink" + System.currentTimeMillis());
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("auto.commit.interval.ms", "1000");
		// add consumer
		final FlinkKafkaConsumer<ObjectNode> myConsumer = new FlinkKafkaConsumer<>("flink_msg", new JSONKeyValueDeserializationSchema(true), props);
		myConsumer.setStartFromEarliest();
		// TODO: 并行度研究

		// 提取时间戳
		final WatermarkStrategy<ObjectNode> watermarkStrategy = WatermarkStrategy
				.<ObjectNode>forMonotonousTimestamps()//forBoundedOutOfOrderness(Duration.ofMillis(10*1000))
				.withTimestampAssigner((SerializableTimestampAssigner<ObjectNode>) (element, recordTimestamp) -> element.get("value").get("log_time").longValue());

		myConsumer.assignTimestampsAndWatermarks(watermarkStrategy);


		// add source
		final DataStreamSource<ObjectNode> input = env.addSource(myConsumer);


		//process
		final SingleOutputStreamOperator<Tuple3<Long, Long, Long>> output = input.process(new ProcessFunction<ObjectNode, Tuple3<Long, Long, Long>>() {
			long currentTimeMillis = Long.MIN_VALUE;
			@Override
			public void processElement(ObjectNode value, Context ctx, Collector<Tuple3<Long,Long, Long>> out) throws Exception {
				final Long logTime = ctx.timestamp();
				final Long envTime = value.get("value").get("log_time").longValue();
				final long currentWatermark = ctx.timerService().currentWatermark();
				out.collect(Tuple3.of(currentWatermark, logTime, envTime));
			}
		});




		// others

		output.print();


		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	/**
	 *  User-defined WindowFunction to compute the average temperature of SensorReadings
	 */
	public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

		/**
		 * apply() is invoked once for each window.
		 *
		 * @param sensorId the key (sensorId) of the window
		 * @param window meta data for the window
		 * @param input an iterable over the collected sensor readings that were assigned to the window
		 * @param out a collector to emit results from the function
		 */
		@Override
		public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) {

			// compute the average temperature
			int cnt = 0;
			double sum = 0.0;
			for (SensorReading r : input) {
				cnt++;
				sum += r.temperature;
			}
			double avgTemp = sum / cnt;

			// emit a SensorReading with the average temperature
			out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
		}
	}
}
