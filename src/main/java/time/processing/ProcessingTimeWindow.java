package time.processing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author : viktor
 * Date  : 2019/11/12 10:45 AM
 * 功能  : processing Time学习
 */
public class ProcessingTimeWindow {

    private static Logger  logger = LoggerFactory.getLogger(ProcessingTimeWindow.class);

    public static void main(String[] args) throws Exception{

        /**
         * 设置env参数
         * local模式启用web UI
         */
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT,8883);
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(1);

        DataStream<String> source = env.socketTextStream("localhost",9888);


        DataStream<Tuple2<String,Long>> flat = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String [] values = value.split(",");
                        for (String s:values) {
                            out.collect(new Tuple2<>(s , 1L));
                        }
                    }
                });


        flat
                .timeWindowAll(Time.minutes(3L))
                .apply(new AllWindowFunction<Tuple2<String,Long>, Tuple3<String,String,Long>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, Long>> values, Collector<Tuple3<String,String,Long>> out) throws Exception {
                        Long sum = 0L;
                        for (Tuple2<String,Long> value:values) {
                            sum +=value.f1;
                        }

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Date start = new Date(window.getStart());
                        Date end = new Date(window.getEnd());


                        out.collect(new Tuple3<>("窗口开始时间："+sdf.format(start),"窗口结束时间："+sdf.format(end),sum));
                    }
                })
                .print();



        env.execute("processing time");
    }
}
