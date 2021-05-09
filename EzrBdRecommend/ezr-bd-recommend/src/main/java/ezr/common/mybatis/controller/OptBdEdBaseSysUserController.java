package ezr.common.mybatis.controller;

import ezr.bigdata.db.ed.EdBaseSysUser;
import ezr.common.mybatis.bean.OptBdEdBaseSysUser;

import java.util.ArrayList;
import java.util.List;

/**
 * 员工信息
 */
public class OptBdEdBaseSysUserController {


    /**
     * 获取门店用户的
     * Id 员工id, BrandId 品牌, MobileNo 手机号, ShopId所属门店
     * @return
     */
    public static ArrayList<OptBdEdBaseSysUser> getOptSysUsers(){
        return EdBaseSysUser.getOptSysUsers();
    }

    /**
     * 获取指定品牌的
     *      * Id 员工id, BrandId 品牌, MobileNo 手机号, ShopId所属门店，员工code
     * @param brandIds
     * @return
     */
    public static ArrayList<OptBdEdBaseSysUser> getSysUsersCode(List<Integer> brandIds){
        return EdBaseSysUser.getSysUsersCode(brandIds);
    }

    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);

        ArrayList<OptBdEdBaseSysUser> lst1 = OptBdEdBaseSysUserController.getOptSysUsers();
        for(OptBdEdBaseSysUser u:lst1){
            System.out.println(u.getId()+"\t"+u.getBrandId()+"\t"+u.getMobileNo()+"\t"+u.getShopId());
        }

        ArrayList<OptBdEdBaseSysUser> lst2 = OptBdEdBaseSysUserController.getSysUsersCode(inList);
        for(OptBdEdBaseSysUser u:lst2){
            System.out.println(u.getId()+"\t"+u.getBrandId()+"\t"+u.getMobileNo()+"\t"+u.getShopId()+"\t"+u.getCode());
        }
    }

}
