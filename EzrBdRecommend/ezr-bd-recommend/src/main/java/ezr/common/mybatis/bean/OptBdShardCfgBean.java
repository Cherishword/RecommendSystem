package ezr.common.mybatis.bean;

/**
 * Created by liucf on 2018/5/10.
 */
public class OptBdShardCfgBean {
    private int id;
    private int shardingId;
    private String dataCenter;
    private int regionCount;
    private int clusterId;
    private int shardingGrpId;
    private String eSHost;
    private int esId;

    public int getEsId() {
        return esId;
    }

    public void setEsId(int esId) {
        this.esId = esId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getShardingId() {
        return shardingId;
    }

    public void setShardingId(int shardingId) {
        this.shardingId = shardingId;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
    }

    public int getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(int regionCount) {
        this.regionCount = regionCount;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public int getShardingGrpId() {
        return shardingGrpId;
    }

    public void setShardingGrpId(int shardingGrpId) {
        this.shardingGrpId = shardingGrpId;
    }

    public String geteSHost() {
        return eSHost;
    }

    public void seteSHost(String eSHost) {
        this.eSHost = eSHost;
    }
}
