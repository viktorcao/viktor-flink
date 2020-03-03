package join.window;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author sanqi
 * @create 2020-02-29 3:03 下午
 * @功能 coGroup测试
 */
public class WindowCoGroup {

    public static void main(String[] args) {

        /**
         * 设置env参数
         * local模式启用web UI
         */
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT,8883);
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(1);


        DataStream<String> source_1 = env.socketTextStream("localhost",9888);
        DataStream<String> source_2 = env.socketTextStream("localhost",9889);

        DataStream<Tuple2<String,String>> UserBehavior = source_1
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {

                        return Tuple2.of(s.split(",")[0],s.split(",")[1]);
                    }
                });

        DataStream<Tuple3<String,String,String>> UserInfo = source_2
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String s) throws Exception {
                        String [] values = s.split(",");
                        return Tuple3.of(values[0],values[1],values[3]);
                    }
                });


        UserBehavior.coGroup(UserInfo)
                .where(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> stringStringTuple2) throws Exception {
                        return stringStringTuple2.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple3<String, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                        return stringStringStringTuple3.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, Object>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> iterable, Iterable<Tuple3<String, String, String>> iterable1, Collector<Object> collector) throws Exception {
                        
                    }
                });

    }
}
