package kason.weixin;

import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhangkai12 on 2017/12/19.
 */
public class KafkaConsumerGet {
    public static void main(String[] args) {

    }

    public static Set<String> getAllGroupForTopic(String brokerListUrl, String topic){

        AdminClient client = AdminClient.createSimplePlaintext(brokerListUrl);
        try {
            List<GroupOverview> allGroups = scala.collection.JavaConversions.seqAsJavaList(client.listAllGroupsFlattened());
            Set<String> groups = new HashSet<>();
            for(GroupOverview overview: allGroups){
                String groupId = overview.groupId();
                //scala.collection.JavaConversions.mapAsJavaMap(client.listGr)
            }
        }finally {

        }
        return null;
    }
}
