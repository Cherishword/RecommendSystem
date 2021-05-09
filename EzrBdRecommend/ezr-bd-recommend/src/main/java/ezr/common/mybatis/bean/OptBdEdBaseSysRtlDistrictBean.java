package ezr.common.mybatis.bean;

public class OptBdEdBaseSysRtlDistrictBean {
    private Integer brandId;  //品牌id
    private Integer shopId; // 片区下的门店id
    private String name; // 片区名称
    private Integer id; // 片区id

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

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
