package ezr.common.mybatis.controller;

import ezr.bigdata.db.bigdata.OptBdMonitorEsMysqlResult;

/**
 * Created by liucf on 2018/12/1.
 */
public class OptBdMonitorEsMysqlResultController {

    /**
     * 更新埋点监控数据结果到mysql监控表
     * 只能监控到count数的
     * @param esCount 数据条数
     * @param esIndexAndType shardId-brandId-indexName-type 拼接的字符串
     */
    public static void updateEsMonitorToMysqlForValue(int esCount,String esIndexAndType){
        OptBdMonitorEsMysqlResult.updateEsMonitorToMysqlForValue(esCount,esIndexAndType);
    }

    /**
     * 更新埋点监控数据结果到mysql监控表
     * @param esCount 数据条数
     * @param esColumOne 第一个字段的sum值
     * @param esColumTwo 第二个字段的sum值
     * @param esIndexAndType shardId-brandId-indexName-type 拼接的字符串
     */
    public static void updateEsMonitorToMysql(int esCount,double esColumOne,double esColumTwo,String esIndexAndType){
        OptBdMonitorEsMysqlResult.updateEsMonitorToMysql(esCount,esColumOne,esColumTwo,esIndexAndType);
    }

    public static void main(String[] args) {
        OptBdMonitorEsMysqlResultController.updateEsMonitorToMysqlForValue(8888,"20190320-1-1-coupon_grp_act-act");
        OptBdMonitorEsMysqlResultController.updateEsMonitorToMysql(100,200.5,300.5,"20190320-1-1-coupon_grp_act-act");
    }
}
