package flinklearning._1source.model;

import java.io.Serializable;
import java.util.Date;

/**
 * 链路信息
 */
public class TraceSegmentRecordInfo implements Serializable {
    /**
     * segmentId
     */
    private String segmentId;
    /**
     * 链路Id
     */
    private String traceId;
    /**
     * 服务名
     */
    private String serviceName;
    /**
     * 服务IP
     */
    private String serviceIp;
    /**
     * 端点名
     */
    private String endpointName;
    /**
     * 加密数据
     */
    private String dataBinary;
    /**
     * 时间20201223105655
     */
    private Long timeBucket;
    /**
     * segment 开始事件
     */
    private Long startTime;
    /**
     * segment 结束时间
     */
    private Long endTime;
    /**
     * 耗时
     */
    private Integer latency;
    /**
     * 是否异常
     */
    private Integer isError;
    /**
     * 创建时间
     */
    private Date createDate;
    /**
     * endpointName() + "-" + traceId
     */
    private String statement;



    public String getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(String segmentId) {
        this.segmentId = segmentId;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceIp() {
        return serviceIp;
    }

    public void setServiceIp(String serviceIp) {
        this.serviceIp = serviceIp;
    }

    public String getEndpointName() {
        return endpointName;
    }

    public void setEndpointName(String endpointName) {
        this.endpointName = endpointName;
    }

    public String getDataBinary() {
        return dataBinary;
    }

    public void setDataBinary(String dataBinary) {
        this.dataBinary = dataBinary;
    }

    public Long getTimeBucket() {
        return timeBucket;
    }

    public void setTimeBucket(Long timeBucket) {
        this.timeBucket = timeBucket;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Integer getLatency() {
        return latency;
    }

    public void setLatency(Integer latency) {
        this.latency = latency;
    }

    public Integer getIsError() {
        return isError;
    }

    public void setIsError(Integer isError) {
        this.isError = isError;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    @Override
    public String toString() {
        return "TraceSegmentRecordInfo{" +
                "segmentId='" + segmentId + '\'' +
                ", traceId='" + traceId + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", serviceIp='" + serviceIp + '\'' +
                ", endpointName='" + endpointName + '\'' +
                ", dataBinary='" + dataBinary + '\'' +
                ", timeBucket=" + timeBucket +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", latency=" + latency +
                ", isError=" + isError +
                ", createDate=" + createDate +
                ", statement='" + statement + '\'' +
                '}';
    }
}
