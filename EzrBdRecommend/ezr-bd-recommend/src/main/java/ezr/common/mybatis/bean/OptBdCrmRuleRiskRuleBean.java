package ezr.common.mybatis.bean;

/**
 * Created by liucf on 2018/5/14.
 */
public class OptBdCrmRuleRiskRuleBean {
    private int id; //系统编号
    private int sumType; //'1 单笔金额\r\n2 累计金额\r\n3 笔数\r\n',
    private int timeType;  //'1 年 \r\n2 月\r\n3 日',
    private int timeValue;  //时间值
    private int value;   //参数值
    private int IsRefundNotInclude; //是否退单不计入计算（消费金额<0的订单不做统计）

    public int getIsRefundNotInclude() {
        return IsRefundNotInclude;
    }

    public void setIsRefundNotInclude(int isRefundNotInclude) {
        IsRefundNotInclude = isRefundNotInclude;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSumType() {
        return sumType;
    }

    public void setSumType(int sumType) {
        this.sumType = sumType;
    }

    public int getTimeType() {
        return timeType;
    }

    public void setTimeType(int timeType) {
        this.timeType = timeType;
    }

    public int getTimeValue() {
        return timeValue;
    }

    public void setTimeValue(int timeValue) {
        this.timeValue = timeValue;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
