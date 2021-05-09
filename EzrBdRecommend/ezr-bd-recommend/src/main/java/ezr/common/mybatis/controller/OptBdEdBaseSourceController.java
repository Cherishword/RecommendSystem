package ezr.common.mybatis.controller;



import ezr.bigdata.db.ed.EdBaseSource;
import ezr.common.mybatis.bean.OptBdEdBaseSourceBean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class OptBdEdBaseSourceController {

    /** 获取 SourceVal,Sort并且是排序后的
     * SELECT SourceVal,Sort FROM ed_base_source
     *  where Sort>=1 ORDER By Sort
     * @return
     */
    public static List<OptBdEdBaseSourceBean> getEdBaseSource() {
        return EdBaseSource.getEdBaseSource();
    }


    /**
     *  把 SourceVal,Sort 让如map
     *  map<SourceVal,Sort>
     * @return
     */
    public static Map<String,Integer> getSortId(){
        return EdBaseSource.getSortId();
    }



    public static void main(String[] args) {
        List<OptBdEdBaseSourceBean> bebs =  OptBdEdBaseSourceController.getEdBaseSource();
        for(OptBdEdBaseSourceBean b: bebs){
            System.out.println(b.getSourceVal()+"\t"+b.getSort());
        }

        Map<String,Integer> map =  OptBdEdBaseSourceController.getSortId();
        for(Map.Entry<String,Integer> e : map.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }
    }
}
