package ezr.common.mybatis.controller;


import ezr.bigdata.db.bigdata.OptBdShardCfg;
import ezr.common.mybatis.bean.OptBdShardCfgBean;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liucf on 2018/5/10.
 * 针对对 mysql opt 库下opt_bd_shard_cfg表的操作
 */
public class OptBdShardCfgController {

    /**
     * 这是一个例子
     */
    public static void selectAll(){
        OptBdShardCfg.selectAll();
    }

    /**
     * 根据shardingGrpId和dataCenter，得到Es服务器地址
     * @param shardingGrpId sharding组的id
     * @return 返回String[] esHostAndPort
     */
    public static String[] getESServers(int shardingGrpId,String dataCenter){
        return OptBdShardCfg.getESServers(shardingGrpId,dataCenter);
    }

    /**
     * 根据ShardingGrpId 取出其包含的多个shardingId的值
     * @param shardingGrpId shardingId 组编号
     * @return 返回拿出的shardingId的列表
     */
    public static  List<Integer>  getShardingIds(int shardingGrpId,String dataCenter){
        return OptBdShardCfg.getShardingIds(shardingGrpId,dataCenter);
    }

    /**
     * 获取ShardGrpId
     * @param shardingId
     * @param dataCenter
     * @return
     */
    public static  int  getShardGrpId(int shardingId,String dataCenter){
        return OptBdShardCfg.getShardGrpId(shardingId,dataCenter);
    }

    /**
     * 根据ShardingGrpId 取出 DataCenter字段
     * @param shardingGrpId shardingId 组编号
     * @return DataCenter字段
     */
    public static  String getDataCenter(int shardingGrpId){
        return OptBdShardCfg.getDataCenter(shardingGrpId);
    }


    /**
     * 根据传入的shardingGrpId获取对应的ShardingGrpId和DataCenter
     * @param shardingGrpId
     * @return 返回 OptBdShardCfgBean对象，切只有ShardingGrpId,DataCenter有实际值
     */
    public static  List<OptBdShardCfgBean> getShardIngAndDataCenter(int shardingGrpId){
        return OptBdShardCfg.getShardIngAndDataCenter(shardingGrpId);
    }

    /**
     * 根据sharing 取出所有 对应的sharinggrpid 和datacenter
     * 注意
     *      这里取到的 sharing sharinggrpid datacenter 的组合并不是一一对应的
     *      所以需要后期使用的时候处理
     * @param shardingIds
     * @return
     */
    public static  List<OptBdShardCfgBean>   getShardGrpAndDataCenter(List<Integer> shardingIds){
        return OptBdShardCfg.getShardGrpAndDataCenter(shardingIds);
    }
    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(100);
        inList.add(234);

        String[] esHostAndPort= OptBdShardCfgController.getESServers(1,"test");
        List<Integer> intResultList = OptBdShardCfgController.getShardingIds(1,"test");
        for(Integer i :intResultList){
            System.out.println(i);
        }
        System.out.println(OptBdShardCfgController.getDataCenter(1));
        System.out.println(OptBdShardCfgController.getShardGrpId(1,"test"));

        List<OptBdShardCfgBean> list =OptBdShardCfgController.getShardGrpAndDataCenter(inList);
        if (list != null){
            for(OptBdShardCfgBean b:list){
                System.out.println(
                        b.getId()+"\t"
                                +b.getEsId()+"\t"
                                +b.getDataCenter()+"\t"
                                +b.getShardingId()+"\t"
                                +b.getShardingGrpId()+"\t"+b.getClusterId());
            }
        }
    }
}
