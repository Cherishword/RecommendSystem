package ezr.bigdata.db.bigdata;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/16 14:26
 */
public class OptBdMonitorJobResult {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.bigDataConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("opt_bd_monitor_job_result");

    public static void updateMonitorResult(String jobName,int status){
        String sqlStr = "update "+TABLE+" set MonitorTime=NOW(),Status= "+status+"  where JobName= '"+jobName+"'";
        System.out.println(sqlStr);
        dbHelper.executeNonQuery(sqlStr);
    }

    public static void main(String[] args) {
        OptBdMonitorJobResult.updateMonitorResult("VipRecruitMinRegTime-1",10);
    }

}
