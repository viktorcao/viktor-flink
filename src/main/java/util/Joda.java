package util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * @author sanqi
 * @create 2020-03-11 11:43 上午
 */
public class Joda {
    /**
     *
     * @param inPattern     输入时间格式
     * @param outPattern    输出时间格式
     * @param value         输入时间字符串
     * @return              转换后的时间字符串
     */
    public  String dateToDate(String inPattern,String outPattern,String value){

        DateTimeFormatter inFormat = DateTimeFormat.forPattern(inPattern);
        DateTimeFormatter outFormat = DateTimeFormat.forPattern(outPattern);

        return DateTime.parse(value,inFormat).toString(outFormat);
    }

    /**
     *
     * @param inPattern     输入时间格式
     * @param value         输入时间字符串
     * @return              时间对应的字符串
     */
    public  Long dateToTimestamp(String inPattern,String value){
        DateTimeFormatter inFormat = DateTimeFormat.forPattern(inPattern);

        return DateTime.parse(value,inFormat).getMillis();
    }


    /**
     *
     * @param outPattern    输出时间格式
     * @param value         时间戳(毫秒)
     * @return              时间字符串
     */
    public  String timestampToDate(String outPattern,Long value){
        DateTimeFormatter outFormat = DateTimeFormat.forPattern(outPattern);
        DateTime dateTime = new DateTime(value);
        return dateTime.toString(outPattern);
    }
}
