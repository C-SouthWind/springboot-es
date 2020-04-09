package com.chj.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.chj.properties.ESStaticProperties.*;


/**
 * @author ：chj
 * @date ：Created in 2020/3/26 20:48
 * @params :
 */
@Configuration
public class ESConfig {
    @Autowired
    private ESProperties esProperties;

    @Bean
    public TransportClient transportClient(){
        //1.创建TransportClient对象
        TransportClient transportClient = null;
        try{
        //2.配置ES(固定写法)
        /*
         * Settings.java
         *
         * Returns a builder to be used in order to build settings.
         *
         *  public static Settings.Builder() {return new Settings.Builder();}
         *  .builder() 返回 Settings.Builder  //就是map
         *
         * private final Map<String, Object> map = new TreeMap<>();  //Setting创建map
         * private SetOnce<SecureSettings> secureSettings = new SetOnce<>();  //Setting创建secureSettings
         *
         *  public Builder put(String key, String value) {
         *             map.put(key, value);
         *             return this;
         *         }
         *  .put(CLUSTER_NAME,esProperties.getClusterName()) //配置ES连接集群
         *  .put(NODE_NAME,esProperties.getNodeName()) //ES集群中的节点名称
         *  .put(THREAD_POOL_SEARCH_SIZE,esProperties.getPool()) //ES集群中最大连接数
         *  .put(CLIENT_TRANSPORT_SNIFF,true) //自动把节点添加到集群
         *
         *  .build() return new Settings(map, secureSettings.get()); //返回Settings
         */
        Settings settings = Settings.builder()
               .put(CLUSTER_NAME,esProperties.getClusterName())
                .put(NODE_NAME,esProperties.getNodeName())
                .put(THREAD_POOL_SEARCH_SIZE,esProperties.getPool())
                .put(CLIENT_TRANSPORT_SNIFF,true).build();
        /*
         * public TransportAddress(InetAddress address, int port) {
         *         this(new InetSocketAddress(address, port));
         *     }
         *
         *  public static InetAddress getByName(String host)
         *         throws UnknownHostException {
         *         return InetAddress.getAllByName(host)[0];
         *     }
         */
        //3.配置ES的连接信息
        TransportAddress transportAddress = new TransportAddress(
                InetAddress.getByName(esProperties.getIpAddr()),esProperties.getPort()
        );
        //4.初始化transportClient对象
        /*
         * public class PreBuiltTransportClient extends TransportClient {}
         * TransportClient extends AbstractClient{}
         * AbstractClient extends AbstractComponent implements Client{}
         * AbstractComponent
         * public AbstractComponent(Settings settings) {
         *         this.logger = Loggers.getLogger(getClass(), settings);
         *         this.deprecationLogger = new DeprecationLogger(logger);
         *         this.settings = settings;
         *     }
         */
       transportClient =  new PreBuiltTransportClient(settings);
       //5.把ES连接信息添加到transportClient对象中
        /*
         * public TransportClient addTransportAddress(TransportAddress transportAddress) {
         *         nodesService.addTransportAddresses(transportAddress);
         *         return this;
         *     }
         */
        transportClient.addTransportAddress(transportAddress);
        }catch(UnknownHostException e){
                e.printStackTrace();
        }
        //6.返回transportClient对象
        return transportClient;
    }
}
