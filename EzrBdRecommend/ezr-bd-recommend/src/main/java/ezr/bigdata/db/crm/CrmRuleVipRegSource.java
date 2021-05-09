package ezr.bigdata.db.crm;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdCrmRuleVipRegSourceBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 22:26
 */
public class CrmRuleVipRegSource {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.crmConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("crm_rule_vip_reg_source");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");
    /**
     * 获取 crm_rule_vip_reg_source 表
     * grpType,platFormId,BrandId,Id 字段
     *
     * SELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in(-100000,1,2,20)
     *  and GrpType='TPS'
     *  ORDER By brandId,id
     *  比如：
     *      TPS	103	1	1551
     *      TPS	120	1	1552
     *      TPS	107	1	1553
     *      TPS	104	1	1554
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdCrmRuleVipRegSourceBean> getVipRegSource(List<Integer> brands, String dataCenter) {
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "SELECT grpType,platFormId,BrandId,Id FROM "+TABLE+" where BrandId in"+inStr+" and GrpType='TPS' ORDER By brandId,id" ;
        }else {
            sqlStr = "SELECT grpType,platFormId,BrandId,id FROM "+TABLE+" where BrandId in"+inStr+" and GrpType='TPS' and dataCenter = '"+dataCenter+"' ORDER By brandId,id" ;
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdCrmRuleVipRegSourceBean> result = new ArrayList<OptBdCrmRuleVipRegSourceBean>();
        try {
            while (rs.next()){
                OptBdCrmRuleVipRegSourceBean crvrs = new OptBdCrmRuleVipRegSourceBean();
                crvrs.setGrpType(rs.getString("grpType"));
                crvrs.setPlatFormId(rs.getInt("platFormId"));
                crvrs.setBrandId(rs.getInt("BrandId"));
                crvrs.setId(rs.getInt("Id"));
                result.add(crvrs);
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
     * 获取 crm_rule_vip_reg_source 表
     * grpType,platFormId,BrandId,Id 字段
     *
     * SELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in(-100000,1,2,20)
     *  and GrpType='TPS'
     *  ORDER By brandId,id
     *  比如：
     *      TPS	103	1	1551
     *      TPS	120	1	1552
     *      TPS	107	1	1553
     *      TPS	104	1	1554
     *   最后把结果封装到map里
     *   map<brandid_id,platFormId>
     *   比如：
     *      1_1552	1
     *      1_1551	0
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String,Integer> getPlatformId(List<Integer> brands, String dataCenter){
        Map<String, Integer> map = new HashMap<String, Integer>();
        Map<Integer,Map<String, Integer>> map1 = new HashMap<Integer,Map<String, Integer>>();
        List<OptBdCrmRuleVipRegSourceBean> list = getVipRegSource(brands,dataCenter);
        String name="";
        String brandAndId="";
        if(!brands.isEmpty()){
            for (int b =0 ; b < brands.size(); b++){
                List<Integer> list1 = new ArrayList<Integer>();
                for (int i =0 ; i < list.size(); i++) {
                    if(list.get(i).getBrandId() == brands.get(b)){
                        list1.add(list.get(i).getId());
                    }
                }
                Collections.sort(list1);


                for (int index=0 ; index<list1.size();index++){
                    map .put (brands.get(b)+"_"+list1.get(index),index);
                }
            }



        }
        return map;
    }


    /**
     * 获取crm_rule_vip_reg_source表的grpType,platFormId,BrandId,Id字段
     * 切根据条件id > 1000 和指定的品牌
     * ELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in (-100000,1,2,20)
     *  and id > 1000
     *  ORDER By id
     *
     *  比如:
     *      SMCP	0	1	1001
     *      GAME	0	1	1002
     *      SMCP	0	1	1003
     *      CQRC	0	1	1004
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdCrmRuleVipRegSourceBean> getVipGrpType(List<Integer> brands, String dataCenter) {
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "";
        if("private".equals(RUN_ENV)){
            sqlStr = "SELECT grpType,platFormId,BrandId,Id FROM "+TABLE+" where BrandId in "+inStr+" and id > 1000 ORDER By id";
        }else {
            sqlStr = "SELECT grpType,platFormId,BrandId,id FROM "+TABLE+" where BrandId in "+inStr+" and id > 1000  and dataCenter = '"+dataCenter+"' ORDER By id";
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdCrmRuleVipRegSourceBean> result = new ArrayList<OptBdCrmRuleVipRegSourceBean>();
        try {
            while (rs.next()){
                OptBdCrmRuleVipRegSourceBean crvrs = new OptBdCrmRuleVipRegSourceBean();
                crvrs.setGrpType(rs.getString("grpType"));
                crvrs.setPlatFormId(rs.getInt("platFormId"));
                crvrs.setBrandId(rs.getInt("BrandId"));
                crvrs.setId(rs.getInt("Id"));
                result.add(crvrs);
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
     * 获取crm_rule_vip_reg_source表的grpType,platFormId,BrandId,Id字段
     * 切根据条件id > 1000 和指定的品牌
     * ELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in (-100000,1,2,20)
     *  and id > 1000
     *  ORDER By id
     *
     *  比如:
     *      SMCP	0	1	1001
     *      GAME	0	1	1002
     *      SMCP	0	1	1003
     *      CQRC	0	1	1004
     *  然后根据grpType 赋值 1-8
     *  结果放到map里
     *  map<brandid_id,1~8>
     *  比如：
     *      1_2199	2
     *      1_2198	2
     *      1_2197	2
     *      1_2196	3
     *      1_2195	2
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String,Integer> getGrpType(List<Integer> brands, String dataCenter){
        Map<String, Integer> map = new HashMap<String, Integer>();
        List<OptBdCrmRuleVipRegSourceBean> list = getVipGrpType(brands,dataCenter);

        String brandAndId="";
        int sortId=0;
        if(!brands.isEmpty()){

            for (int i = 0; i < list.size(); i++) {
                Integer id = list.get(i).getId();
                String grpType = list.get(i).getGrpType();
                Integer brandId = list.get(i).getBrandId();
                brandAndId=brandId+"_"+id;
                if(grpType.equals("API")){
                    sortId=1;
                }else if(grpType.equals("SMCP")){
                    sortId=2;
                }else if(grpType.equals("GAME")){
                    sortId=3;
                }else if(grpType.equals("CQRC")){
                    sortId=4;
                }else if(grpType.equals("MDYQ")){
                    sortId=5;
                }else if(grpType.equals("ZDJC")){
                    sortId=6;
                }
                else if(grpType.equals("TPS")){
                    sortId=8;
                }else {
                    sortId=7;
                }

                map.put(brandAndId,sortId);

            }

        }
        return map;
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);

        List<OptBdCrmRuleVipRegSourceBean>  lst1 = CrmRuleVipRegSource.getVipRegSource(inList,"test");
        for(OptBdCrmRuleVipRegSourceBean c:lst1){
            //grpType,platFormId,BrandId,Id
            System.out.println(c.getGrpType()+"\t"+c.getPlatFormId()+"\t"+c.getBrandId()+"\t"+c.getId());
        }

        Map<String,Integer> m = CrmRuleVipRegSource.getPlatformId(inList,"test");
        for(Map.Entry<String,Integer> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }

        List<OptBdCrmRuleVipRegSourceBean>  lst2 = CrmRuleVipRegSource.getVipGrpType(inList,"test");
        for(OptBdCrmRuleVipRegSourceBean c:lst2){
            //grpType,platFormId,BrandId,Id
            System.out.println(c.getGrpType()+"\t"+c.getPlatFormId()+"\t"+c.getBrandId()+"\t"+c.getId());
        }

        Map<String,Integer> m2 = CrmRuleVipRegSource.getGrpType(inList,"test");
        for(Map.Entry<String,Integer> e:m2.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }
    }
}
