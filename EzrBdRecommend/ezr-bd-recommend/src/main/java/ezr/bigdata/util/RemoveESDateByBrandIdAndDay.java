package ezr.bigdata.util;


import ezr.common.mybatis.controller.OptBdEdBaseBrandController;
import ezr.common.mybatis.controller.OptBdShardCfgController;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by bigdata on 2018/04/28.
 * 使用java 调用shell脚本使用spark-submit是不能提交的，只能使用java -cp 提交
 * 删除es上的数据,脚本如下：
 * 异常订单删除es
 * curl -XDELETE 192.168.12.181:9200/exsale$1/risk/_query?pretty -d '{"query": {"bool": {"must":[{"term": {"brandId": "'$2'"}},{"term": {"hitTime": "'$3'"}}]}}}'
 *
 * 消费频次删除es数据
 * curl -XDELETE 192.168.12.181:9200/vip_frequency$1/sale/_query?pretty -d '{"query": {"bool": {"must":[{"term": {"brandId": "'$2'"}},{"term": {"day": "'$3'"}}]}}}'
 * curl -XDELETE 192.168.12.181:9200/vip_frequency$1/all/_query?pretty -d '{"query": {"bool": {"must":[{"term": {"brandId": "'$2'"}},{"term": {"day": "'$3'"}}]}}}'
 * curl -XDELETE 192.168.12.181:9200/vip_active$1/all/_query?pretty -d '{"query": {"bool": {"must":[{"term": {"brandId": "'$2'"}},{"term": {"day": "'$3'"}}]}}}'
 *
 */
public class RemoveESDateByBrandIdAndDay {
    public static void runLinuxShell(String dir,String brandid,String shardingGrpId,String yestoday){
        InputStream in = null;
        String strCmd = "sh "+dir+" "+shardingGrpId+" "+brandid+" "+yestoday;
        System.out.println(strCmd);
        try {
            Process pro = Runtime.getRuntime().exec(strCmd);
            pro.waitFor();
            in = pro.getInputStream();
            BufferedReader read = new BufferedReader(new InputStreamReader(in));
            String result = null;
            System.out.println("INFO: brandid is "+brandid +" and hitTime = "+yestoday+", date delete!");
            while ((result = read.readLine()) != null){
                System.out.println("INFO: "+result);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){
        String shardingGrpId = args[0];
        String today = args[1];
        System.out.println(today);
        //参数传递今天的时间
        DateTime day = new DateTime(today);
        System.out.println(day);
        //往前推一天
        String yestoday = day.minusDays(1).toString("yyyyMMdd");

        System.out.println(yestoday);
        String dir = args[2];
        String dataCenter = OptBdShardCfgController.getDataCenter(Integer.valueOf(shardingGrpId));
        String[] serversAndPort = OptBdShardCfgController.getESServers(Integer.valueOf(shardingGrpId),dataCenter);
        //List<String> servers = BaseESShardNodesDao.getESServers(esShardingId);//servers= 192.168.12.181:9200
        String esServer = serversAndPort[0].split(",")[0];

        System.out.println(esServer);

        /**根据ShardingGrpId 取出其包含的多个shardingId的值*/
        List<Integer> shardingIds = OptBdShardCfgController.getShardingIds(Integer.valueOf(shardingGrpId),dataCenter);
        /**根据shardingId 获取对应的品牌*/
        List<Integer> brands= OptBdEdBaseBrandController.getBrandIds(shardingIds);
        if(null != brands){
            for(int brandId:brands){
                RemoveESDateByBrandIdAndDay.runLinuxShell(dir, String.valueOf(brandId),shardingGrpId,yestoday);
            }
        }
    }
}
