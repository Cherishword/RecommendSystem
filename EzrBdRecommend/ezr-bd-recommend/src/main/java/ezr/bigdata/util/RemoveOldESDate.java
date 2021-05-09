package ezr.bigdata.util;


import ezr.common.mybatis.controller.OptBdEdBaseBrandController;
import ezr.common.mybatis.controller.OptBdJobStatusController;
import ezr.common.mybatis.controller.OptBdShardCfgController;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by bigdata on 2017/12/13.
 * 使用java 调用shell脚本使用spark-submit是不能提交的，只能使用java -cp 提交
 */
public class RemoveOldESDate {
    public static void runLinuxShell(String dir,String brandid,String shardingId){
        InputStream in = null;
        String strCmd = "sh "+dir+" "+shardingId+" "+brandid;
        System.out.println("cmd:  "+strCmd);
        try {
            Process pro = Runtime.getRuntime().exec(strCmd);
            pro.waitFor();
            in = pro.getInputStream();
            BufferedReader read = new BufferedReader(new InputStreamReader(in));
            String result = null;
            System.out.println("INFO: brandid is "+brandid +", date delete!");
            while ((result = read.readLine()) != null){
                System.out.println("INFO: "+result);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){
        String shardingGrpId = args[0];
        String dir = args[1];
        String dataCenter = OptBdShardCfgController.getDataCenter(Integer.valueOf(shardingGrpId));
        /**拿到所有需要计算的品牌*/
        List<Integer>brandIds = OptBdJobStatusController.getBrandIds(dataCenter);
        if(null==brandIds || brandIds.isEmpty()){
            System.out.println("brandIds is isEmpty!");
            System.exit(-1);
        }else {
            System.out.println("List<Integer>brandIds");
            for(int brandid:brandIds){
                System.out.println(brandid);
            }
        }

        /**拿到这个shardingGrpId组对应的shardingId的集合*/
        List<Integer>shardingIds = OptBdShardCfgController.getShardingIds(Integer.valueOf(shardingGrpId),dataCenter);
        if(null==shardingIds || shardingIds.isEmpty()){
            System.out.println("shardingIds is isEmpty!");
            System.exit(-1);
        } else{
            System.out.println(" List<Integer>shardingIds");
            for(int shardingid :shardingIds){
                System.out.println(shardingid);
            }
        }
        /**拿到这个shardingGrpId组对应的切需要计算的品牌*/
        List<Integer>list2 = OptBdEdBaseBrandController.getBrandIds(shardingIds,brandIds);
        if(null != list2 && list2.size() !=0){
            System.out.println("size "+list2.size());

            for(int brandId:list2){
                System.out.println(brandId+ " data deleteing...........");
                RemoveOldESDate.runLinuxShell(dir, String.valueOf(brandId),shardingGrpId);
            }
        }else {
            System.out.println("status=0  &&  shardingGrpId="+shardingGrpId+" is null !");
        }
        System.out.println("end.....");
    }
}
