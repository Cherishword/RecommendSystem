package ezr.common.mybatis.controller;

import ezr.bigdata.db.crm.CrmRuleVipGrade;
import ezr.common.mybatis.bean.OptBdCrmRuleVipGradeBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OptBdCrmRuleVipGradeController {

    /** 根据指定品牌获取等级的gradeId,brandId,gradeName
     * select Id,BrandId,Name from crm_rule_vip_grade
     * where BrandId in (-100000,1,2,20)
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdCrmRuleVipGradeBean> getByName(List<Integer> brands, String dataCenter) {
        return CrmRuleVipGrade.getByName(brands,dataCenter);
    }

    /**
     *根据指定品牌获取等级的gradeId,brandId,gradeName
     *      * select Id,BrandId,Name from crm_rule_vip_grade
     *      * where BrandId in (-100000,1,2,20)
     *  然后把结果封装成map
     *  map<brandId_Id,name>
     *
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String, String> getGradeName(List<Integer> brands, String dataCenter){
        return CrmRuleVipGrade.getGradeName(brands,dataCenter);
    }

    /**
     * 根据指定的品牌获取 gradeId，brandId，LevelId
     *   select Id,BrandId,LevelId from crm_rule_vip_grade
     *   WHERE BrandId in (-100000,1,2,20)
     * @param brandIds
     * @return
     */
    public static ArrayList<OptBdCrmRuleVipGradeBean> getVipGradeLevel(List<Integer> brandIds){
        return CrmRuleVipGrade.getVipGradeLevel(brandIds);
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptBdCrmRuleVipGradeBean> lst = OptBdCrmRuleVipGradeController.getByName(inList,"test");
        for(OptBdCrmRuleVipGradeBean c :lst){
            System.out.println(c.getId()+"\t"+c.getBrandId()+"\t"+c.getName());
        }

        Map<String, String>  m = OptBdCrmRuleVipGradeController.getGradeName(inList,"test");
        for(Map.Entry<String, String> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }

        ArrayList<OptBdCrmRuleVipGradeBean> lst2 = OptBdCrmRuleVipGradeController.getVipGradeLevel(inList);
        for(OptBdCrmRuleVipGradeBean c:lst2){
            System.out.println(c.getId()+"\t"+c.getBrandId()+"\t"+c.getLevelId());
        }

    }
}
