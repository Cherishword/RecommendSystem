package ezr.common.mybatis.bean;

/**
 * Created by liucf on 2018/5/14.
 */
public class OptBdJobStatusBean {
    private int id;
    private int brandId;
    private String dataCenter;
    private int copId;
    private String name;
    private int status;
    private int validVipCount;
    private String comment;
    private String updateTime;

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public int getBrandId() {
        return brandId;
    }

    public void setBrandId(int brandId) {
        this.brandId = brandId;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
    }

    public int getCopId() {
        return copId;
    }

    public void setCopId(int copId) {
        this.copId = copId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getValidVipCount() {
        return validVipCount;
    }

    public void setValidVipCount(int validVipCount) {
        this.validVipCount = validVipCount;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
