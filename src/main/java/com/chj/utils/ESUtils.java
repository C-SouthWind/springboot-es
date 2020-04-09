package com.chj.utils;


import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ：chj
 * @date ：Created in 2020/3/27 9:52
 * @params :
 */
@Component
public final class ESUtils {
    /**
     * @Autowired  一定是在spring的配置文件全部加载完毕之后才生效，注入
     *
     *  静态会在类加载的时候加载
     *  如果TransportClient用静态修饰     ESConfig类 配置不会生效TransportClient对象是空的 会空指针异常
     */
    @Autowired
    private TransportClient transportClient;

    private static TransportClient client;
    /**
     *     HashMap   不锁
     *     HashTable   全锁
     *     ConcurrentHashMap  用到那锁那
     *
     *  class ---  ConcurrentHashMap<K,V> extends AbstractMap<K,V> implements ConcurrentMap<K,V>, Serializable
     * abstract class --- AbstractMap<K,V> implements Map<K,V>
     * interface --- ConcurrentMap<K, V> extends Map<K, V>
     */
    private static Map<String,Object> resultMap = new ConcurrentHashMap<String, Object>();


   /** 方法描述
   * @Description:  在依赖注入之后  自动调用@PostConstruct   配置加载完改为静态
   * @Param: []
   * @return: void
   * @Author: chj
   * @Date: 2020/3/27
   */
    @PostConstruct
    public void init(){
        //把非静态变量赋值给静态变量  解决静态方法无法引用非静态变量
        client = transportClient;
    }

    /** 方法描述
    * @Description: 向ES中创建index
    * @Param: [index]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Map<String,Object> createIndex(String index){
        //1.从ES中判断index是否存在
        if (indexExist(index)) {
            //index存在
            /*
             * put无论存在不存在都覆盖key
             * putIfAbsent   key存在就不覆盖，不存在就存入
             */
            resultMap.put("code","10001");
            resultMap.put("msg","index存在");
        }else {
            /*
             * .prepareCreate(index)
             * abstract class  ---- TransportClient extends AbstractClient
             *              static class IndicesAdmin implements IndicesAdminClient
             *                          interface----- IndicesAdminClient extends ElasticsearchClient
             *                                   index The index name to create//要创建的索引名
             *                                   CreateIndexRequestBuilder prepareCreate(String index);
             *
             * .prepareCreate(index) 添加index
             * 。execute（）  提交事务
             */
            CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(index).execute().actionGet();
            //Returns whether the response is acknowledged or not//返回是否确认响应
            if (createIndexResponse.isAcknowledged()) {
                    //创建成功
                resultMap.put("code","20001");
                resultMap.put("msg","index创建成功");
            }else{
                resultMap.put("code", "10002");
                resultMap.put("msg", "index创建失败");
            }
        }
        return resultMap;
    }


    /** 方法描述
    * @Description: 删除index
    * @Param: [index]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Map<String,Object> deleteIndex(String index){
        //index存在
        /*if (indexExist(index)) {
            resultMap.put("code", "10001");
            resultMap.put("msg", "index存在");
        }else{*/
            /*
             * .prepareDelete(index)
             *   abstract class  ---- TransportClient extends AbstractClient
             *              static class IndicesAdmin implements IndicesAdminClient
             *                          interface----- IndicesAdminClient extends ElasticsearchClient
             *                                   The indices to delete. Use "_all" to delete all indices.//要删除的索引名
             *                                   DeleteIndexRequestBuilder prepareDelete(String... indices);
             * prepareDelete（index） 删除
             */
        if (indexExist(index)) {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
                //返回是否响应
            if (deleteIndexResponse.isAcknowledged()) {
                resultMap.put("code", "20002");
                resultMap.put("msg", "index删除成功");
            }else{
                resultMap.put("code", "10003");
                resultMap.put("msg", "index删除失败");
            }
        }else{
            resultMap.put("code", "10001");
            resultMap.put("msg", "index不存在");
        }
       // }
        return resultMap;
    }

    /** 方法描述
    * @Description: 对象转map
    * @Param: [object]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/4/3
    */
    public static Map<String,Object> objectTurnMap(Object object){
        Map<String,Object> result = new HashMap<String, Object>();
        Field[] fields = object.getClass().getDeclaredFields();
        try{
         for (Field field: fields) {
            field.setAccessible(true);
            String name = new String(field.getName());
            result.put(name,field.get(object));
        }
        }catch (IllegalAccessException e){
            e.printStackTrace();
        }
        return result;
    }




   /** 方法描述
   * @Description: 判断ES中是否存在index
   * @Param: [index]
   * @return: java.lang.Boolean
   * @Author: chj
   * @Date: 2020/3/27
   */
    public  static Boolean indexExist(String index){
        /*
         * .admin()  获取ES中的最大权限
         *          class --- TransportClient extends AbstractClient
         *          abstract class --- AbstractClient
         *                  static class Admin implements AdminClient {
         *                      private final ClusterAdmin clusterAdmin;
         *                      private final IndicesAdmin indicesAdmin;
         *                      //TODO
         *                  }
         *  .indices()  //允许对索引进行操作
         *                  interface ---  AdminClient{
         *                          //A client allowing to perform actions/operations against the cluster.//允许对集群进行操作
         *                          lusterAdminClient cluster();
         *                          // A client allowing to perform actions/operations against the indices.//允许对索引执行操作
         *                          IndicesAdminClient indices();
         *                  }
         *  .exists(new IndicesExistsRequest(index))  存在索引请求
         *              static class IndicesAdmin implements IndicesAdminClient{
         *                   //The indices exists request  存在索引请求
         *                  ActionFuture<IndicesExistsResponse> exists(IndicesExistsRequest request);
         *                  //TODO
         *              }
         *  .actionGet(); //获取返回结果
         *      interface --- ActionFuture<T> extends Future<T>
         *       T actionGet();
         */
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        return indicesExistsResponse.isExists();
    }

    /** 方法描述
    * @Description: 判断指定的index下的type是否存在
    * @Param: [index, type]
    * @return: java.lang.Boolean
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Boolean isTypeExist(String index,String type){
            /*
             * .prepareTypesExists(index)
             *   abstract class  ---- TransportClient extends AbstractClient
             *              static class IndicesAdmin implements IndicesAdminClient
             *                          interface----- IndicesAdminClient extends ElasticsearchClient
             *                                   Indices exists.//指数存在
             *                                   TypesExistsRequestBuilder prepareTypesExists(String... index);
             * .setType(type)
             *               class ---- TypesExistsRequestBuilder
             *                  types The types to check if they exist//检查类型是否存在
             *                   public TypesExistsRequestBuilder setTypes(String... types) {//TODO}
             */
            return indexExist(index)?
                    client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists():
                    false;
        }

    /** 方法描述
    * @Description: 向ES中存入数据
    * @Param: [mapObj, index, type, uuid]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Map<String,Object>  addData(Map<String,Object> mapObj,String index,String type ,String uuid){
                //index,type不存在 ES会自动生成这些东西，不需要判断是否存在
            /*
             * .prepareIndex(index,type,uuid)
             *       abstract class  ------- TransportClient extends AbstractClient
             *       abstract class --------  AbstractClient extends AbstractComponent implements Client
             *       interface--------------- Client extends ElasticsearchClient, Releasable
             *                      Index a document associated with a given index and type.//索引与给定索引和类型关联的文档
             *                      The id is optional, if it is not provided, one will be generated automatically.//id是可选的，如果没有提供，将自动生成一个id
             *                      index The index to index the document to//为文档建立索引的索引
             *                      type  The type to index the document to//键入要为文档建立索引的类型
             *                      id    The id of the document//id文件的id
             *                      IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id);
             * .setSource(mapObj)
             *       class ------------------ IndexRequestBuilder extends ReplicationRequestBuilder<IndexRequest, IndexResponse, IndexRequestBuilder>
             *   implements WriteRequestBuilder<IndexRequestBuilder>
             *                          Index the Map as a JSON.//将映射索引为JSON。
             *                          public IndexRequestBuilder setSource(Map<String, ?> source) {//TODO}
             *.get()
             *          Short version of execute().actionGet().//execute().actionGet()的缩写
             *          public Response get() {return execute().actionGet();}
             */
            IndexResponse indexResponse = client.prepareIndex(index, type, uuid).setSource(mapObj).get();
            /*
             * class-------------- IndexResponse extends DocWriteResponse
             * abstract class -----DocWriteResponse extends ReplicationResponse implements WriteResponse, StatusToXContentObject
             * interface----------- StatusToXContentObject extends ToXContentObject
             *                      Returns the REST status to make sure it is returned correctly//返回REST状态，以确保正确返回
             *                      RestStatus status();
              */
            //判断数据是否添加成功
            String status = indexResponse.status().toString().toUpperCase();
            if("CREATED".equals(status)||"OK".equals(status)) {
                // 数据添加成功
                resultMap.put("code", "20003");
                resultMap.put("msg", "数据添加成功");
            } else {
                resultMap.put("code", "10004");
                resultMap.put("msg", "数据添加失败");
            }
            return resultMap;
        }
    /** 方法描述
    * @Description: UUID自动生成  不自己指定
    * @Param: [mapObj, index, type]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Map<String,Object>  addData(Map<String,Object> mapObj,String index,String type ){
        /*
         * java.util.UUID
         * .randomUUID()
         * class ------- UUID implements java.io.Serializable, Comparable<UUID>
         *              A randomly generated {@code UUID}//返回一共随机生成的code
         *              public static UUID randomUUID() {//TODO}
         *  .replaceAll（）
         *              把所有  "-"  替换成  ""
         *  .toUpperCase()
         *              转为大写
         */
        //mysql中uuid做主键  携带 - 在mysql中无法查出来 用的98版本SQL标准 不是02年的SQL标准
        String uuid = UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
        return addData(mapObj, index, type,uuid);
    }

    /** 方法描述
    * @Description: 通过uuid删除数据
    * @Param: [index, type, uuid]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Map<String,Object> deleteDataByUUID(String index,String type,String uuid){
            /*
             * .prepareIndex(index,type,uuid)
             *       abstract class  ------- TransportClient extends AbstractClient
             *       abstract class --------  AbstractClient extends AbstractComponent implements Client
             *       interface--------------- Client extends ElasticsearchClient, Releasable
             *
             *                  Deletes a document from the index based on the index, type and id.//根据索引、类型和id从索引中删除文档。
             *                  index The index to delete the document from//索引要从中删除文档的索引
             *                  type  The type of the document to delete//键入要删除的文档的类型
             *                  id    The id of the document to delete//要删除的文档的id
             *                  DeleteRequestBuilder prepareDelete(String index, String type, String id);
             *
             */
            DeleteResponse deleteResponse = client.prepareDelete(index, type, uuid).get();
            //状态 、转大写
            String status = deleteResponse.status().toString().toUpperCase();
            if ("OK".equals(status)) {
                resultMap.put("code", "20004");
                resultMap.put("msg", "数据删除成功");
            } else {
                resultMap.put("code", "10005");
                resultMap.put("msg", "数据删除失败");
            }
            return resultMap;
    }

    /** 方法描述
    * @Description: 通过uuid修改ES中的数据
    * @Param: [index, type, uuid, mapObj]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Map<String,Object> updateDataByUUID(String index,String type,String uuid,Map<String,Object> mapObj){
        //创建修改请求
        UpdateRequest updateRequest = new UpdateRequest();
        //链式调用
        updateRequest.index(index).type(type).id(uuid).doc(mapObj);
        /*
         *
         *       abstract class  ------- TransportClient extends AbstractClient
         *       abstract class --------  AbstractClient extends AbstractComponent implements Client
          *      interface--------------- Client extends ElasticsearchClient, Releasable
         *
         *              Updates a document based on a script.//基于脚本更新文档。
         *               ActionFuture<UpdateResponse> update(UpdateRequest request);
         */
        //修改、返回结果数据、转大写
        String status = client.update(updateRequest).actionGet().status().toString().toUpperCase();
        if ("OK".equals(status)) {
            resultMap.put("code", "20005");
            resultMap.put("msg", "数据修改成功");
        } else {
            resultMap.put("code", "10006");
            resultMap.put("msg", "数据修改失败");
        }
        return resultMap;
    }

    /** 方法描述
    * @Description: 通过uuid从es中查询数据
    * @Param: [index, type, uuid, fields]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Map<String,Object> queryDataByUUID(String index,String type,String uuid,String fields){
        /*
         * .prepareGet(index, type, uuid)
         *      abstract class  ------- TransportClient extends AbstractClient
         *      abstract class --------  AbstractClient extends AbstractComponent implements Client
         *      interface--------------- Client extends ElasticsearchClient, Releasable
         *                  Gets the document that was indexed from an index with a type (optional) and id.//获取从具有类型(可选)和id的索引中建立索引的文档。
         *                   GetRequestBuilder prepareGet(String index, @Nullable String type, String id);
         */
        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, uuid);
        if (null != fields && !"".equals(fields)) {
                //.setFetchSource(fields.split(","),null)  设置包含或者排查
                //excludes:排除
                getRequestBuilder.setFetchSource(fields.split(","),null);
        }
        //提交、返回结果
        GetResponse actionGet = getRequestBuilder.execute().actionGet();
        //返回Map
        return actionGet.getSource();
    }

    /** 方法描述
    * @Description: 从ES中关键字搜索一个集合
    *               query:ES给java提供的查询方法
    *               size:具体要显示多少条
    *               fields:所需要显示的字段
    *               sortFields:所需要排序的字段
    *                highListghtField:高亮显示
    * @Param: [index, type, query, size, fields, sortFields, highListghtField]
    * @return: java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static List<Map<String, Object>> queryListData(
            String index, String type, QueryBuilder query, Integer size, String fields,
            String sortFields, String highListghtField) {
        /*
         *        abstract class  ------- TransportClient extends AbstractClient
         *        abstract class --------  AbstractClient extends AbstractComponent implements Client
         *        interface--------------- Client extends ElasticsearchClient, Releasable
         *                  Search across one or more indices and one or more types with a query.//使用查询跨一个或多个索引和一个或多个类型进行搜索
         *                      SearchRequestBuilder prepareSearch(String... indices);
         */
        //1.创建出索引对象
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        //2.判断是不是又type
        if (null != type && !"".equals(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        //3.是否有高亮显示
        if(null != highListghtField && !"".equals(highListghtField)) {
            // 获取高亮显示的设置对象
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            // 具体设置哪些字段需要高亮显示
            highlightBuilder.field(highListghtField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }
        //设置查询方法 查询所有、模糊查询、分页查询、分词查询
        searchRequestBuilder.setQuery(query);
        // 4.判断是否显示所有字段
        if(null != fields && !"".equals(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        // 5.是否有字段需要排序
        if(null != sortFields && !"".equals(sortFields)) {
            // SortOrder.DESC:排序的方式  DESC:倒序 ASC:正序(也可以用形参的形式来传入)
            searchRequestBuilder.addSort(sortFields, SortOrder.DESC);
        }

        // 6.是否有条件查询
        if(null != size && 0 < size) {
            searchRequestBuilder.setSize(size);
        }

        // 7.执行查询
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        // searchResponse.getHits():就是查询返回的结果集--->其实真正咱们所需要的结果就是SearchHit(uuid,type,source(就是咱们所需要的数据))
        Long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;
        System.out.println("TotalHits:"+ totalHits);
        System.out.println("length:"+ length);
        // 8.判断是否查询成功
        if(200 == searchResponse.status().getStatus()) {
            // 说明查询成功
            // 怎么获取数据呢？？-->高亮显示需要特殊处理
            return dealSearchResponse(searchResponse, highListghtField);
        }
        return null;
    }

    /** 方法描述
    * @Description: 处理高亮显示结果
    * @Param: [searchResponse, highListghtField]
    * @return: java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
    * @Author: chj
    * @Date: 2020/3/27
    */
    private static List<Map<String, Object>> dealSearchResponse(SearchResponse searchResponse, String highListghtField) {
        // 1.定义装载咱们最终所需要结果的List集合
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        // 2.使用StringBuilder进行拼接(效率是最高-->绝对要优于String拼接)
        StringBuilder stringBuilder = new StringBuilder();
        // 3.循环searchResponse获取最终结果
        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());
            // 4.判断是否真正的传过来了高亮显示(double check)
            if(null != highListghtField && !"".equals(highListghtField)) {
                System.out.println("查看高亮显示，并且覆盖原有的结果(从ES中查询出来的结果中并没有高亮显示)"+searchHit.getSourceAsMap());
                // 使用高亮显示来覆盖原有的从ES中查询出来的普通结果(添加上样式)
                Text[] texts = searchHit.getHighlightFields().get(highListghtField).getFragments();
                if(null != texts) {
                    // 5.遍历高亮结果，并且拼接到StringBuilder中
                    for(Text text : texts) {
                        stringBuilder.append(text.string());
                    }
                    // 覆盖了所需要返回的结果
                    searchHit.getSourceAsMap().put(highListghtField, stringBuilder.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }
        return sourceList;
    }

}
