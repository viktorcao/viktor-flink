package time.event;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author sanqi
 * @create 2020-03-02 3:13 下午
 * 功能 window学习
 */
public class EventTimeWindowB {

    private static Logger logger = LoggerFactory.getLogger(EventTimeWindowB.class);

    public static void main(String[] args) throws Exception {

        /**
         * 设置env参数
         * local模式启用web UI
         */
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT,8883);
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        /**
         *
         */

        //OutputTag<Tuple4<String, String, Long, Long>> lateOutputTag = new OutputTag<>("lateness-data");

        DataStream<String> sourceDS = env
                .socketTextStream("localhost" , 9999)
                .uid("socket-9999")
                .name("socket-9999");

        DataStream<Tuple3<String,String,Long>> assignDS = sourceDS
                .map(new MapFunction<String, Tuple3<String,String,Long>>() {
                    @Override
                    public Tuple3<String,String,Long> map(String s) throws Exception {
                        JSONObject object = JSON.parseObject(s);


                        return new Tuple3<String,String,Long>(
                                object.getString("name"),
                                object.getString("behavior"),
                                getTimestamp(object.getString("time"))
                        );
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)){
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                });



        DataStream<Tuple4<String, String, String, Long>>  resultDS= assignDS
                .map(new MapFunction<Tuple3<String, String, Long>, Tuple4<String, String, Long,Long>>() {
                    @Override
                    public Tuple4<String, String, Long, Long> map(Tuple3<String, String, Long> s) throws Exception {
                        logger.info(s.toString() + "=====" + getTime(s.f2));
                        return new Tuple4<>(s.f0,s.f1,s.f2,1L);
                    }
                })
                .timeWindowAll(Time.minutes(1))
                .process(new ProcessAllWindowFunction<Tuple4<String, String, Long, Long>, Tuple4<String, String, String, Long>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple4<String, String, Long, Long>> elements, Collector<Tuple4<String, String, String, Long>> out) throws Exception {

                    }
                });


        resultDS.print();

        env.execute("watermark 试试水");

    }

    /**
     *
     * @param str
     * @return timeStamp
     * @throws ParseException
     * 字符串时间(yyyy-MM-dd hh:mm:ss)转时间戳
     */
    public static long getTimestamp(String str) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(str);
        long ts = date.getTime();
        return ts;
    }


    /**
     *
     * @param ts
     * @return date
     * 时间戳转日期格式(yyyy-MM-dd hh:mm:ss)
     */
    public static String getTime(Long ts){
        Date date = new Date(ts);
        return date.toString();
    }



}
