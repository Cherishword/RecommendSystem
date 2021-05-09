package ezr.bigdata.util;


import ezr.common.mybatis.controller.OptBdEdBaseBrandController;
import ezr.common.mybatis.controller.OptBdJobStatusController;
import ezr.common.mybatis.controller.OptBdShardCfgController;
import ezr.common.mybatis.controller.OptJobLastChangeController;

import java.util.List;

/**
 * Created by bigdata on 2018/12/03.
 * 使用java 调用shell脚本使用spark-submit是不能提交的，只能使用java -cp 提交
 *
 * 需要重新计算历史会员线上化的的时候
 * 删除 opt_job_lastchange 表的jobName 匹配的记录
 * 以达到清除上次计算条件遗留下来的数据
 */
public class RemoveOldESDateForWeChatAHV {
    public static void main(String[] args){
        String shardingGrpId = args[0];
        String jobName = args[1];
        String dataCenter = OptBdShardCfgController.getDataCenter(Integer.valueOf(shardingGrpId));
        /**拿到所有需要计算的所有品牌*/
        List<Integer>brandIds = OptBdJobStatusController.getBrandIds(dataCenter);
        if(null==brandIds || brandIds.isEmpty()){
            System.out.println("" +
                    "brandIds is isEmpty!");
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
            System.out.println("" +
                    "shardingIds is isEmpty!");
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
            OptJobLastChangeController.deleteJobName(jobName+shardingGrpId);
            System.out.println("The "+jobName+shardingGrpId+" was deleted !....................");

        }else {
            System.out.println("status=0  &&  shardingGrpId="+shardingGrpId+" is null !");
        }
        System.out.println("end.....");
    }
}
