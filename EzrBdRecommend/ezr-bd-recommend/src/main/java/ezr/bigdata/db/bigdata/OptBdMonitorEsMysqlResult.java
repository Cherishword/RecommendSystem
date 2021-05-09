package ezr.bigdata.db.bigdata;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/16 14:36
 */
public class OptBdMonitorEsMysqlResult {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.bigDataConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("opt_bd_monitor_es_mysql_result");

    /**
     * 更新埋点监控数据结果到mysql监控表
     * 只能监控到count数的
     * @param esCount 数据条数
     * @param esIndexAndType shardId-brandId-indexName-type 拼接的字符串
     */
    public static void updateEsMonitorToMysqlForValue(int esCount,String esIndexAndType){

        String sqlStr = "update "+TABLE+" set esCount="+esCount+", esDateInsterTime=NOW() where esIndexAndType='"+esIndexAndType+"'";
        System.out.println(sqlStr);
        dbHelper.executeNonQuery(sqlStr);
    }

    /**
     * 更新埋点监控数据结果到mysql监控表
     * @param esCount 数据条数
     * @param esColumOne 第一个字段的sum值
     * @param esColumTwo 第二个字段的sum值
     * @param esIndexAndType shardId-brandId-indexName-type 拼接的字符串
     */
    public static void updateEsMonitorToMysql(int esCount,double esColumOne,double esColumTwo,String esIndexAndType){
        String sqlStr = "update "+TABLE+" set esCount="+esCount+" ,esColumOne="+esColumOne+", esColumTwo="+esColumTwo+", esDateInsterTime=NOW() where esIndexAndType='"+esIndexAndType+"'";
        System.out.println(sqlStr);
        dbHelper.executeNonQuery(sqlStr);
    }
}
