package kason.kafkamonitor.entity;

/**
 * Created by zhangkai12 on 2017/12/26.
 */
public class MetadataInfo {
    private int partitionId;
    private int leader;
    private String isr;
    private String replicas;

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public String getIsr() {
        return isr;
    }

    public void setIsr(String isr) {
        this.isr = isr;
    }

    public String getReplicas() {
        return replicas;
    }

    public void setReplicas(String replicas) {
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "MetadataInfo{" +
                "partitionId=" + partitionId +
                ", leader=" + leader +
                ", isr='" + isr + '\'' +
                ", replicas='" + replicas + '\'' +
                '}';
    }
}
