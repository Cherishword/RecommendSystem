package ezr.bigdata.db.crm;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdCrmRuleVipGradeBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 18:14
 */
public class CrmRuleVipGrade {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.crmConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("crm_rule_vip_grade");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");

    /** 根据指定品牌获取等级的gradeId,brandId,gradeName
     * select Id,BrandId,Name from crm_rule_vip_grade
     * where BrandId in (-100000,1,2,20)
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdCrmRuleVipGradeBean> getByName(List<Integer> brands, String dataCenter) {
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "select Id,BrandId,Name from " + TABLE + " where BrandId in " + inStr ;
        }else {
            sqlStr = "select Id,BrandId,Name from " + TABLE + " where BrandId in " + inStr + " and dataCenter = '" + dataCenter + "'";
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdCrmRuleVipGradeBean> result = new ArrayList<OptBdCrmRuleVipGradeBean>();
        try {
            while (rs.next()) {
                OptBdCrmRuleVipGradeBean crvg = new OptBdCrmRuleVipGradeBean();
                crvg.setId(rs.getInt("Id"));
                crvg.setBrandId(rs.getInt("BrandId"));
                crvg.setName(rs.getString("Name"));
                result.add(crvg);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return result;
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
    public static Map<String, String> getGradeName(List<Integer> brands, String dataCenter) {
        Map<String, String> map = new HashMap<String, String>();
        List<OptBdCrmRuleVipGradeBean> list = getByName(brands, dataCenter);
        String name = "";
        String brandAndId = "";
        if (!brands.isEmpty()) {
            for (OptBdCrmRuleVipGradeBean bean : list) {
                Integer id = bean.getId();
                Integer brand = bean.getBrandId();
                if ("".equals(bean.getName()) || bean.getName() == null) {
                    name = "null";
                } else {
                    name = bean.getName();
                }
                brandAndId = brand + "_" + id;
                map.put(brandAndId, name);
            }

        }
        return map;
    }

    /**
     * 根据指定的品牌获取 gradeId，brandId，LevelId
     *   select Id,BrandId,LevelId from crm_rule_vip_grade
     *   WHERE BrandId in (-100000,1,2,20)
     * @param brandIds
     * @return
     */
    public static ArrayList<OptBdCrmRuleVipGradeBean> getVipGradeLevel(List<Integer> brandIds) {
        String inStr = EzrStringUtil.getInStr(brandIds);
        String sqlStr = "select Id,BrandId,LevelId from " + TABLE + " WHERE BrandId in " + inStr;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        ArrayList<OptBdCrmRuleVipGradeBean> list = new ArrayList<OptBdCrmRuleVipGradeBean>();
        try {
            while (rs.next()) {
                OptBdCrmRuleVipGradeBean crvg = new OptBdCrmRuleVipGradeBean();
                crvg.setId(rs.getInt("Id"));
                crvg.setBrandId(rs.getInt("BrandId"));
                crvg.setLevelId(rs.getInt("LevelId"));
                list.add(crvg);
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
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptBdCrmRuleVipGradeBean> lst = CrmRuleVipGrade.getByName(inList,"test");
        for(OptBdCrmRuleVipGradeBean c :lst){
            System.out.println(c.getId()+"\t"+c.getBrandId()+"\t"+c.getName());
        }

        Map<String, String>  m = CrmRuleVipGrade.getGradeName(inList,"test");
        for(Map.Entry<String, String> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }

        ArrayList<OptBdCrmRuleVipGradeBean> lst2 = CrmRuleVipGrade.getVipGradeLevel(inList);
        for(OptBdCrmRuleVipGradeBean c:lst2){
            System.out.println(c.getId()+"\t"+c.getBrandId()+"\t"+c.getLevelId());
        }

    }
}
