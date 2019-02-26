package com.whp;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.whp.bolt.SenqueceBolt;
import com.whp.scheme.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;

public class StormKafkaTopology {
    public static void main(String[] args) {
        BrokerHosts brokerHosts = new ZkHosts("10.4.66.91:2181,10.4.66.92:2181,10.4.66.93:2181");

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "hdfs_audit_log", "/storm", "audit_log");

        Config conf = new Config();
        Map<String, String> map = new HashMap<String, String>();

        map.put("metadata.broker.list", "10.4.66.91:9092,10.4.66.92:9092,10.4.66.93:9092");
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        conf.put("topic", "hdfs_audit_event_sandbox");

        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout");
//        builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");

        if (args != null && args.length > 0) {
            //提交到集群运行
            try {
                StormSubmitter.submitTopology("storm-kafka-toplogy", conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            //本地模式运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-kafka-toplogy", conf, builder.createTopology());
            Utils.sleep(1000000);
            cluster.killTopology("storm-kafka-toplogy");
            cluster.shutdown();
        }
    }
}
