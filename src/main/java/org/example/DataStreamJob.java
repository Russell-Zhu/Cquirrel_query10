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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.Relations.Customer;
import org.example.Relations.LineItem;
import org.example.Relations.Nation;
import org.example.Relations.Order;
import org.example.operators.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    private static final OutputTag<LineItem> lineItemOutputTag = new OutputTag<LineItem>("lineItem") {
    };
    private static final OutputTag<Order> ordersOutputTag = new OutputTag<Order>("orders") {
    };
    private static final OutputTag<Customer> customerOutputTag = new OutputTag<Customer>("customer") {
    };
    private static final OutputTag<Nation> nationOutputTag = new OutputTag<Nation>("nation") {
    };

    public static Date startdate = Date.valueOf("1992-10-01");
    public static Date enddate = Date.valueOf(Date.valueOf("1992-10-01").toLocalDate().plusMonths(3));
    long millis1 = System.currentTimeMillis();
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.addSource(new DataSource()).setParallelism(1);

        // Split input stream into different Relations
        SingleOutputStreamOperator<String> processStream = stream.process(new SplitStreamFunc());
        DataStream<Customer> customerDataStream = processStream.getSideOutput(customerOutputTag);
        DataStream<Order> orderDataStream = processStream.getSideOutput(ordersOutputTag);
        DataStream<LineItem> lineItemDataStream = processStream.getSideOutput(lineItemOutputTag);
        DataStream<Nation> nationDataStream = processStream.getSideOutput(nationOutputTag);

        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US);// 1998-09-14

//		StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("data/output"), new SimpleStringEncoder<>("UTF-8"))
//				.withRollingPolicy(
//						DefaultRollingPolicy.builder()
//								.withRolloverInterval(TimeUnit.SECONDS.toMillis(30))
//								.withInactivityInterval(TimeUnit.SECONDS.toMillis(15))
//								.withMaxPartSize(1024 * 1024 * 1024)
//								.build())
//				.build();

        env.setParallelism(2);
//		SingleOutputStreamOperator<Tuple11<Boolean, Integer, Integer, String, Float, String, String, String, Double, Double, String>> COLStream = customerDataStream
//				.connect(orderDataStream)
//				.keyBy((c) -> c.custKey, (o) -> o.custKey)
//				.process(new CustomerOrderJoin())
//				.filter(t -> t.f9.compareTo(startdate) >= 0
//						&& t.f9.compareTo(enddate) < 0)
//				.connect(lineItemDataStream)
//				.keyBy((t) -> t.f8, (l) -> l.orderKey)
//				.process(new COLineItemJoin())
//				.filter(t -> t.f10.equals("R"));
////				.keyBy((t) -> t.f2 + simpleDateFormat.format(t.f4) + t.f5)
////				.flatMap(new AggFunction())
////				.print();
//		nationDataStream
//				.connect(COLStream)
//				.keyBy((n) -> n.nationKey, (t) -> t.f2)
//				.process(new NationCOLJoin())
//				.keyBy((t) -> t.f1 + t.f2 + t.f3 + t.f4 + t.f5 + t.f6 + t.f7)
//				.flatMap(new AggFunction())
//				.print();
        nationDataStream
                .connect(customerDataStream)
                .keyBy((n) -> n.nationKey, (c) -> c.nationKey)
                .process(new NationCustomerJoin())
                .connect(orderDataStream)
                .keyBy((t) -> t.f1, (o) -> o.custKey)
                .process(new NCOrderJoin())
                .filter(t -> t.f9.compareTo(startdate) >= 0
                        && t.f9.compareTo(enddate) < 0)
                .connect(lineItemDataStream)
                .keyBy((t) -> t.f8, (l) -> l.orderKey)
                .process(new NCOLineItemJoin())
                .filter(t -> t.f10.equals("R"))
                .keyBy((t) -> t.f1 + t.f2 + t.f3 + t.f4 + t.f5 + t.f6 + t.f7)
                .flatMap(new AggFunction())
                .print();

        env.execute();
    }

    public static class SplitStreamFunc extends ProcessFunction<String, String> {
        @Override
        public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
            String[] fields = s.split("\\|");
            String tag = fields[1];
            switch (tag) {
                case "customer":
                    context.output(customerOutputTag, new Customer(fields));
                    break;
                case "orders":
                    context.output(ordersOutputTag, new Order(fields));
                    break;
                case "lineitem":
                    context.output(lineItemOutputTag, new LineItem(fields));
                    break;
                case "nation":
                    context.output(nationOutputTag, new Nation(fields));
                    break;
                default:
                    break;
            }
        }
    }


    public static class AggFunction extends RichFlatMapFunction<
            Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>,
            String> {
        private AggregatingState<Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>, Double> revenueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            revenueState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<>(
                            "sum",
                            new AggregateFunction<>() {
                                @Override
                                public Double createAccumulator() {
                                    return 0.0;
                                }

                                @Override
                                public Double add(Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String> tuple, Double acc) {
                                    if (tuple.f0) {
                                        return acc + tuple.f8 * (1 - tuple.f9);
                                    } else {
                                        return acc - tuple.f8 * (1 - tuple.f9);
                                    }
                                }

                                @Override
                                public Double getResult(Double result) {
                                    return result;
                                }

                                @Override
                                public Double merge(Double d1, Double d2) {
                                    return null;
                                }
                            },
                            Types.DOUBLE
                    )
            );
        }

        @Override
        public void flatMap(Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String> tuple,
                            Collector<String> collector) throws Exception {
            revenueState.add(tuple);
            Tuple9<Boolean, Integer, String, String, Float, String, String, String, String> innerResult
                    = Tuple9.of(tuple.f0, tuple.f1, tuple.f2, String.format("%.2f", revenueState.get()), tuple.f3, tuple.f4, tuple.f5, tuple.f6, tuple.f7);
            collector.collect(innerResult.toString());
        }
    }
}
