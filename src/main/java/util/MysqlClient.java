package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author : 恋晨
 * Date  : 2019/11/8 10:59 AM
 * 功能  :
 */
public class MysqlClient {

    private static Logger logger = LoggerFactory.getLogger(MysqlClient.class);

    public static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(
                    "jdbc:mysql://10.128.1.25:3306/sbg?useUnicode=true&characterEncoding=UTF-8"
                    , "root"
                    , "sb_pass");

            logger.info("=============连接深巴sbg成功===================");

        } catch (Exception e) {
            logger.info(e.getMessage(), e);
        }
        return con;
    }
}
