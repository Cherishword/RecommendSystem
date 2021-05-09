package ezr.common.mybatis.bean;

import java.io.Serializable;

/**
 * @author liuchangfu@easyretailpro.com
 * 2019/11/21.
 */
public class EdBaseBrand implements Serializable{
    private int id;
    private String code;
    private String name;
    private char isActive;
    private int crmDbShardingId;
    private int brandId;
    private String dataCenter;

    public String getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
    }



    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public char getIsActive() {
        return isActive;
    }

    public void setIsActive(char isActive) {
        this.isActive = isActive;
    }

    public int getCrmDbShardingId() {
        return crmDbShardingId;
    }

    public void setCrmDbShardingId(int crmDbShardingId) {
        this.crmDbShardingId = crmDbShardingId;
    }

    public int getBrandId() {
        return brandId;
    }

    public void setBrandId(int brandId) {
        this.brandId = brandId;
    }
}
