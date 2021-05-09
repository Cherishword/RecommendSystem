package ezr.bigdata.db.crm;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.common.mybatis.bean.OptBdCrmRuleRiskRuleBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 17:47
 */
public class CrmRuleRiskrule {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.crmConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("crm_rule_riskrule");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");
    /**
     * 拿到品牌对应的异常订单的定义
     * @param brandId 指定品牌
     * @return 返回异常订单定义信息
     */
    public static ArrayList<OptBdCrmRuleRiskRuleBean> getRuleRiskrule(int brandId, String dataCenter){
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "select Id,SumType,TimeType,TimeValue,Value,IsRefundNotInclude from "+TABLE+" where BrandId = "+brandId+" and DataType = 2 ";
        }else {
            sqlStr = "select Id,SumType,TimeType,TimeValue,Value,IsRefundNotInclude from "+TABLE+" where BrandId = "+brandId+" and DataType = 2 and dataCenter = '"+dataCenter+"'";;
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        ArrayList<OptBdCrmRuleRiskRuleBean>list = new ArrayList<OptBdCrmRuleRiskRuleBean>();
        try {
            while (rs.next()){
                OptBdCrmRuleRiskRuleBean crrr = new OptBdCrmRuleRiskRuleBean();
                crrr.setId(rs.getInt("Id"));
                crrr.setSumType(rs.getInt("SumType"));
                crrr.setTimeType(rs.getInt("TimeType"));
                crrr.setTimeValue(rs.getInt("TimeValue"));
                crrr.setValue(rs.getInt("Value"));
                crrr.setIsRefundNotInclude(rs.getInt("IsRefundNotInclude"));
                list.add(crrr);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return list;
    }

    public static void main(String[] args) {
        ArrayList<OptBdCrmRuleRiskRuleBean>  lst = CrmRuleRiskrule.getRuleRiskrule(1,"test");
        for(OptBdCrmRuleRiskRuleBean b:lst){
            System.out.println(b.getId()+"\t"+b.getSumType()+"\t"+b.getTimeType()+"\t"+b.getTimeValue()+"\t"+b.getValue()+"\t"+b.getIsRefundNotInclude());
        }
    }
}
