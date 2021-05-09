package ezr.bigdata.db.crm;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 17:17
 */
public class CrmRuleBonusSell {

    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.crmConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("crm_rule_bonus_sell");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");
    /**
     * 获取对应品牌的积分过期预警时间
     * @param brands 品牌
     * @return 返回依品牌为key,过期预警时间为value的map
     */
    public static Map<Integer,Integer> getExpireWarnDays(List<Integer> brands, String dataCenter){
        Map<Integer,Integer> map = new HashMap<Integer,Integer>();
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "select BrandId,ExpireWarnDays from "+TABLE+" where brandId in "+inStr;
        }else {
            sqlStr = "select BrandId,ExpireWarnDays from "+TABLE+" where brandId in "+inStr+" and dataCenter = 'test' ";
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        try {
            while (rs.next()){
                ;
                map.put(rs.getInt("BrandId"),rs.getInt("ExpireWarnDays"));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return map;
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        Map<Integer,Integer> m = CrmRuleBonusSell.getExpireWarnDays(inList,"test");
        for(Map.Entry<Integer,Integer> e : m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }
    }

}
