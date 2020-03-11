package time.event;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Joda;


import java.util.Properties;

/**
 * @author sanqi
 * @create 2020-03-11 2:14 下午
 */
public class LatenessWindowA {

    private static Logger logger = LoggerFactory.getLogger(LatenessWindowA.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        /**
         * 设置kafka参数
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "dev");
        /** key 反序列化 */
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /** value 反序列化 */
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        DataStream<String> source = env.addSource(new FlinkKafkaConsumer09<String>(
                "user_behavior",
                new SimpleStringSchema(),
                properties
        ).setStartFromEarliest());

        DataStream<Tuple6<String,String,String,String,String,Long>> assginDataStream = source
                .map(new MapFunction<String, Tuple6<String, String, String, String, String, Long>>() {
                    @Override
                    public Tuple6<String, String, String, String, String, Long> map(String s) throws Exception {
                        Joda joda = new Joda();
                        JSONObject object = JSON.parseObject(s);
                        return new Tuple6<>(
                                object.getString("user_id"),
                                object.getString("item_id"),
                                object.getString("category_id"),
                                object.getString("behavior"),
                                object.getString("ts"),
                                joda.dateToTimestamp("yyyy-MM-dd'T'HH:mm:ss'Z'",object.getString("ts"))
                        );
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple6<String, String, String, String, String, Long>>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple6<String, String, String, String, String, Long> element) {
                        return element.f5;
                    }
                });

        assginDataStream
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.minutes(30)))
            .aggregate(new AggregateFunction<Tuple6<String, String, String, String, String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                           @Override
                           public Tuple2<String, Long> createAccumulator() {
                               return new Tuple2<>("", 0L);
                           }

                           @Override
                           public Tuple2<String, Long> add(Tuple6<String, String, String, String, String, Long> stringStringStringStringStringLongTuple6, Tuple2<String, Long> stringLongTuple2) {
                               stringLongTuple2.f0 = stringStringStringStringStringLongTuple6.f0;
                               stringLongTuple2.f1 += 1;

                               return stringLongTuple2;
                           }

                           @Override
                           public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
                               String User_id = stringLongTuple2.f0;
                               Long cnt = stringLongTuple2.f1;
                               return new Tuple2<>(User_id, cnt);
                           }

                           @Override
                           public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
                               return new Tuple2<>(stringLongTuple2.f0, stringLongTuple2.f1 + acc1.f1);
                           }
                       },
                    new ProcessWindowFunction<Tuple2<String, Long>, Tuple4<String,String,String,Long>, Tuple, TimeWindow>() {
                        @Override
                        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
                            Joda joda = new Joda();
                            Tuple2<String, Long> acc = elements.iterator().next();
                            Tuple4<String, String, String, Long> result = new Tuple4<>(
                                    acc.f0,
                                    joda.timestampToDate("yyyy-MM-dd HH:mm:ss",context.window().getStart()),
                                    joda.timestampToDate("yyyy-MM-dd HH:mm:ss",context.window().getEnd()),
                                    acc.f1
                            );

                            out.collect(result);
                        }
                    }
            ).print();

        env.execute("test");

    }
}
