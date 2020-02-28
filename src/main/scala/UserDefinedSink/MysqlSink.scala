package UserDefinedSink


import java.sql.{Connection, PreparedStatement}
import util.MysqlClient

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

/**
  *
  * @author : 恋晨
  *         Date  : 2019/11/8 10:54 AM
  *         功能  :
  *
  */
class MysqlSink  extends  RichSinkFunction[Row]{

  final  val log = LoggerFactory.getLogger("basRealTimeLineSink")
  private var connection:Connection = _
  private var ps:PreparedStatement = _



  override def open(parameters: Configuration): Unit ={
    connection = MysqlClient.getConnection()
    //super.open(parameters)
  }

  override def close(): Unit = {
    //super.close()
    try{
      /** 关闭连接和释放资源*/
      if (connection != null) {
        connection.close();
      }
      if (ps != null) {
        ps.close();
      }

    }catch {
      case e: Exception => log.error(e.getMessage)

    }
  }


  override def invoke(value:Row, context: SinkFunction.Context[_]): Unit = {
    try{



      val insertSql = "INSERT INTO pay_card(id,line_name,bus_no,card_no,card_kind,discount_amount,txn_amount,txn_price,txn_date,txn_type,txn_kind,run_date,create_time,modify_time,org_name,parent_org_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

      ps = connection.prepareStatement(insertSql)


      /**组装数据，执行插入操作*/

      ps.setString(1,value.toString)


      ps.executeUpdate()

      log.info("插入数据     " + value.toString)

    }catch {
      case e: Exception => log.error(e.getMessage)
    }
  }

}
