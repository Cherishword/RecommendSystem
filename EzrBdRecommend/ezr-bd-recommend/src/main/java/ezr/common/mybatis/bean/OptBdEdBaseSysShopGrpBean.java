package ezr.common.mybatis.bean;

public class OptBdEdBaseSysShopGrpBean {
    private Integer brandId;
    private Integer shopId;
    private String name;
    private Integer shopGrpId;

    public Integer getBrandId() {
        return brandId;
    }

    public void setBrandId(Integer brandId) {
        this.brandId = brandId;
    }

    public Integer getShopId() {
        return shopId;
    }

    public void setShopId(Integer shopId) {
        this.shopId = shopId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getShopGrpId() {
        return shopGrpId;
    }

    public void setShopGrpId(Integer shopGrpId) {
        this.shopGrpId = shopGrpId;
    }
}
