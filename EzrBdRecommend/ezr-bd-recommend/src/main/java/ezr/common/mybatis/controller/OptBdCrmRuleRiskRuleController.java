package ezr.common.mybatis.controller;

import ezr.bigdata.db.crm.CrmRuleRiskrule;
import ezr.common.mybatis.bean.OptBdCrmRuleRiskRuleBean;

import java.util.ArrayList;

/**
 * Created by liucf on 2018/5/14.
 */
public class OptBdCrmRuleRiskRuleController {
    /**
     * 拿到品牌对应的异常订单的定义
     * select Id,SumType,TimeType,TimeValue,Value,IsRefundNotInclude
     * from crm_rule_riskrule
     * where BrandId = 1
     * and DataType = 2
     * @param brandId 指定品牌
     * @return 返回异常订单定义信息
     */
    public static ArrayList<OptBdCrmRuleRiskRuleBean> getRuleRiskrule(int brandId,String dataCenter){
        return CrmRuleRiskrule.getRuleRiskrule(brandId,dataCenter);
    }

    public static void main(String[] args) {
        ArrayList<OptBdCrmRuleRiskRuleBean>  lst = OptBdCrmRuleRiskRuleController.getRuleRiskrule(1,"test");
        for(OptBdCrmRuleRiskRuleBean b:lst){
            System.out.println(b.getId()+"\t"+b.getSumType()+"\t"+b.getTimeType()+"\t"+b.getTimeValue()+"\t"+b.getValue()+"\t"+b.getIsRefundNotInclude());
        }
    }

}
