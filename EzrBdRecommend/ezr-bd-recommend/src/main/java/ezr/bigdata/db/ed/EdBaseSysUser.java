package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdEdBaseSysUser;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 员工信息
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 14:34
 */
public class EdBaseSysUser {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("ed_base_sys_user");
//    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");

    /**
     * 获取门店用户的
     * Id 员工id, BrandId 品牌, MobileNo 手机号, ShopId所属门店
     * @return
     */
    public static ArrayList<OptBdEdBaseSysUser> getOptSysUsers(){
        String sqlStr = "SELECT Id, BrandId, MobileNo, ShopId FROM "+TABLE+" WHERE UserType='SH'";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        ArrayList<OptBdEdBaseSysUser> list = new ArrayList<OptBdEdBaseSysUser>();
        try {
            while (rs.next()){
                OptBdEdBaseSysUser ebsu = new OptBdEdBaseSysUser();
                ebsu.setId(rs.getInt("Id"));
                ebsu.setBrandId(rs.getInt("BrandId"));
                ebsu.setMobileNo(rs.getString("MobileNo"));
                ebsu.setShopId(rs.getInt("ShopId"));
                list.add(ebsu);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return list;
    }

    /**
     * 获取指定品牌的
     *      * Id 员工id, BrandId 品牌, MobileNo 手机号, ShopId所属门店，员工code
     * @param brandIds
     * @return
     */
    public static ArrayList<OptBdEdBaseSysUser> getSysUsersCode(List<Integer> brandIds){
        String inStr = EzrStringUtil.getInStr(brandIds);
        String sqlStr = "SELECT Id, BrandId, MobileNo, ShopId,Code FROM "+TABLE+" WHERE BrandId in"+inStr;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        ArrayList<OptBdEdBaseSysUser> list = new ArrayList<OptBdEdBaseSysUser>();
        try {
            while (rs.next()){
                OptBdEdBaseSysUser ebsu = new OptBdEdBaseSysUser();
                ebsu.setId(rs.getInt("Id"));
                ebsu.setBrandId(rs.getInt("BrandId"));
                ebsu.setMobileNo(rs.getString("MobileNo"));
                ebsu.setShopId(rs.getInt("ShopId"));
                ebsu.setCode(rs.getString("Code"));
                list.add(ebsu);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return list;
    }

    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);

        ArrayList<OptBdEdBaseSysUser> lst1 = EdBaseSysUser.getOptSysUsers();
        for(OptBdEdBaseSysUser u:lst1){
            System.out.println(u.getId()+"\t"+u.getBrandId()+"\t"+u.getMobileNo()+"\t"+u.getShopId());
        }

        ArrayList<OptBdEdBaseSysUser> lst2 = EdBaseSysUser.getSysUsersCode(inList);
        for(OptBdEdBaseSysUser u:lst2){
            System.out.println(u.getId()+"\t"+u.getBrandId()+"\t"+u.getMobileNo()+"\t"+u.getShopId()+"\t"+u.getCode());
        }
    }

}
