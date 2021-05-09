package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptEdRuleCusVipInfoConBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 15:37
 */
public class EdRuleCusVipIndiceCon {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("ed_rule_cus_vip_indice_con");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");
    /**
     * 根据传进来的brandid的列表 取出指定字段的值封装到EdRuleCusVipInfoConBean对象里
     * @param indiceId 要取出的字段值
     * @param brands brandid的列表
     * @return 返回 EdRuleCusVipInfoConBean对象的列表
     */
    public static List<OptEdRuleCusVipInfoConBean> getByIndiceId(String indiceId, List<Integer> brands, String dataCenter){
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "select Id,BrandId,ConVal from "+TABLE+" where IndiceId = "+indiceId+" and BrandId in "+inStr;
        }else {
            sqlStr = "select Id,BrandId,ConVal from "+TABLE+" where IndiceId = "+indiceId+" and BrandId in "+inStr+" and dataCenter = '"+dataCenter+"'";
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptEdRuleCusVipInfoConBean> result = new ArrayList<OptEdRuleCusVipInfoConBean>();
        try {
            while (rs.next()){
                OptEdRuleCusVipInfoConBean rcic = new OptEdRuleCusVipInfoConBean();
                rcic.setId(rs.getInt("Id"));
                rcic.setBrandId(rs.getInt("BrandId"));
                rcic.setConVal(rs.getString("ConVal"));
                result.add(rcic);
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
     * 根据brandid 取得每个品牌流失会员的定义
     * @param brands  List<Integer> brands
     * @return Map<Integer, Integer> map
     */
    public static Map<Integer, Integer > getLostDaysOfBrands(List<Integer> brands, String dataCenter) {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        List<OptEdRuleCusVipInfoConBean> list = getByIndiceId("26",brands,dataCenter);
        if(!brands.isEmpty()){
            for(int brand:brands){
                int id = 0;
                String conVal = "0";
                for(OptEdRuleCusVipInfoConBean bean:list){
                    if(brand == bean.getBrandId() && bean.getId() > id){//同一个品牌下，最大的id号对应着会员流失天数
                        id = bean.getId();
                        conVal = bean.getConVal();
                    }
                }
                map.put(brand,Integer.valueOf(conVal));
            }
        }
        return map;
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptEdRuleCusVipInfoConBean>  c = EdRuleCusVipIndiceCon.getByIndiceId("26",inList,"test");

        for(OptEdRuleCusVipInfoConBean cc : c){
            System.out.println(cc.getId()+"\t"+cc.getBrandId()+"\t"+cc.getConVal());
        }

        Map<Integer, Integer > m = EdRuleCusVipIndiceCon.getLostDaysOfBrands(inList,"test");
        for(Map.Entry<Integer, Integer > e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }

    }
}
