package ezr.bigdata.db.bigdata;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import org.joda.time.DateTime;

import java.sql.ResultSet;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/13 18:36
 */
public class OptLastChange {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.bigDataConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("opt_job_lastchange");

    /**
     * 获取 opt_job_lastchange 表 JobName字段和LastModifyDate字段的值
     * @param jobName
     * @return
     */
    public static String getLastChangeDate(String jobName){
        String sqlStr = "SELECT DATE_FORMAT(LastModifyDate,'%Y-%m-%d %H:%i:%s') LastModifyDate FROM "+TABLE+" where JobName = '" + jobName + "' limit 1 ";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getStringResult(rs,"LastModifyDate");
    }

    /**
     * 更新opt_job_lastchange表 的JobName字段对应的LastModifyDate字段
     * @param jobName
     */
    public static void updateLastChange(String jobName){
        String sqlStr = "INSERT INTO "+TABLE+" (JobName,LastModifyDate) VALUES ('" + jobName + "','" + DateTime.now().toString("yyyy-MM-dd HH:mm:ss") + "') ON DUPLICATE KEY UPDATE LastModifyDate = '" + DateTime.now().toString("yyyy-MM-dd HH:mm:ss") + "'" ;
        System.out.println(sqlStr);
        dbHelper.executeNonQuery(sqlStr);
    }


    public static void deleteJobName(String jobName){
        String sqlStr = "delete from "+TABLE+" where JobName='"+jobName+"'";
        System.out.println(sqlStr);
        dbHelper.executeNonQuery(sqlStr);
    }

    public static void main(String[] args){
        System.out.println(OptLastChange.getLastChangeDate("hive_VipFirstSale1"));
        OptLastChange.updateLastChange("hive_VipFirstSale1");
        System.out.println(OptLastChange.getLastChangeDate("hive_VipFirstSale1"));
    }
}
