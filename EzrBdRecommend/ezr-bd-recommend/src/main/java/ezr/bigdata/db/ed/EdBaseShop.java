package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdEdBaseShopBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/14 21:18
 */
public class EdBaseShop {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("ed_base_shop");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");


    /**
     * 根据品牌获取门店信息
     * select Id,BrandId,Name,Code from ed_base_shop
     *  where BrandId in (-100000,1,2,20)
     *
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdEdBaseShopBean> getByName(List<Integer> brands, String dataCenter) {
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "select Id,BrandId,Name,Code from "+TABLE+" where BrandId in "+inStr;
        }else {
            sqlStr = "select Id,BrandId,Name,Code from "+TABLE+" where BrandId in "+inStr+" and dataCenter = '"+dataCenter+"'";
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdEdBaseShopBean> result = new ArrayList<OptBdEdBaseShopBean>();
        try {
            while (rs.next()){
                OptBdEdBaseShopBean ebsb = new OptBdEdBaseShopBean();
                ebsb.setId(rs.getInt("Id"));
                ebsb.setBrandId(rs.getInt("BrandId"));
                ebsb.setName(rs.getString("Name"));
                ebsb.setCode(rs.getString("Code"));
                result.add(ebsb);
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
     * 根据品牌获取门店名字
     * select Id,Name from ed_base_shop
     *  where BrandId in (-100000,1,2,20)
     *
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String, String> getShopName(List<Integer> brands, String dataCenter){
        Map<String, String> map = new HashMap<String, String>();
        List<OptBdEdBaseShopBean> list = getByName(brands,dataCenter);
        String name="";
        String brandAndId="";
        if(!brands.isEmpty()){
            for(OptBdEdBaseShopBean bean :list){
                Integer id = bean.getId();
                Integer brand = bean.getBrandId();
                if("".equals(bean.getName())||bean.getName()==null){
                    name="null";
                }else {
                    name=bean.getName();
                }
                brandAndId=brand+"_"+id;
                map.put(brandAndId,name);
            }

        }
        return map;
    }

    /**
     * 根据品牌获取门店Code
     * select Id,Code from ed_base_shop
     *  where BrandId in (-100000,1,2,20)
     *
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String, Integer> getShopCode(List<Integer> brands,String dataCenter){
        Map<String, Integer> map = new HashMap<String, Integer>();
        List<OptBdEdBaseShopBean> list = getByName(brands,dataCenter);
        if(!brands.isEmpty()){
            for(OptBdEdBaseShopBean bean :list){
                String code = bean.getCode();
                Integer brand = bean.getBrandId();
                String brandAndCode=brand+"_"+code;
                map.put(brandAndCode,bean.getId());
            }

        }
        return map;
    }
}
