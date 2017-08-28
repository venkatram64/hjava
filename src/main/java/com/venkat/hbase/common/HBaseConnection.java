package com.venkat.hbase.common;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * Created by venkatram.veerareddy on 8/28/2017.
 */

@Configuration
public class HBaseConnection {

    private volatile Connection connection;

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String zookeeperClientPort;

    @Value("${hbase.master}")
    private String hbaseMaster;

    @Value("${zookeeper.znode.parent}")
    private String zookeeperZnodeParent;

    @Value("{zookeeper.admin}")
    private String admin;

    @PostConstruct
    public void openConnection() throws IOException{
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort",zookeeperClientPort);
        conf.set("hbase.master",hbaseMaster);
        conf.set("zookeeper.znode.parent",zookeeperZnodeParent);
        //conf.set("zookeeper.admin",admin);
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(admin));
        UserGroupInformation.setConfiguration(conf);
        connection = ConnectionFactory.createConnection(conf);
    }

    public Connection getConnection(){
        return connection;
    }

    public void closeConnection(){
        if(connection != null){
            try{
                connection.close();
            }catch (IOException e){
                throw new RuntimeException("Unable to close connection", e);
            }
        }
    }
}
