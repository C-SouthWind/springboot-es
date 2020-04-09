package com.chj.service;

import com.chj.mapper.BookMapper;
import com.chj.model.Book;
import com.chj.utils.ESUtils;
import com.chj.utils.NotEmpty;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.chj.properties.ESStaticProperties.INDEX_TEST;
import static com.chj.properties.ESStaticProperties.TYPE_TEST;

/**
 * @author ：chj
 * @date ：Created in 2020/3/27 17:10
 * @params :
 */
@Service
public class ESService {

    @Autowired
    private BookMapper bookMapper;

    public static Map<String,Object> resultMap = new ConcurrentHashMap<String, Object>();



    /** 方法描述
    * @Description: 向ES中创建index
    * @Param: [index]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public  Map<String,Object> createIndex(String index){
        if (NotEmpty.stringNotEmpty(index)) {
            return ESUtils.createIndex(index);
        }
            resultMap.put("code","4010");
            resultMap.put("msg","index不能为空");
        return resultMap;

    }
    /** 方法描述
    * @Description: 删除index
    * @Param: [index]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public  Map<String,Object> deleteIndex(String index){
        if (NotEmpty.stringNotEmpty(index)) {
            return ESUtils.deleteIndex(index);
        }
        resultMap.put("code","4011");
        resultMap.put("msg","index不能为空");
        return resultMap;
    }
    /** 方法描述
    * @Description: 向ES中存入数据
    * @Param: [mapObj, index, type, uuid]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public  Map<String,Object>  addData(Map<String,Object> mapObj,String index,String type ,String uuid){
        if (NotEmpty.mapNotEmpty(mapObj)  && NotEmpty.stringNotEmpty(index) && NotEmpty.stringNotEmpty(type)) {
            return ESUtils.addData(mapObj,index,type,uuid);
        }
        resultMap.put("code","4012");
        resultMap.put("msg","mapObj, index, type不能为空");
        return resultMap;

    }
    /** 方法描述
    * @Description: 向ES中存入数据
    * @Param: [mapObj, index, type]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public  Map<String,Object>  addData(Map<String,Object> mapObj,String index,String type ){
        if (NotEmpty.mapNotEmpty(mapObj)  && NotEmpty.stringNotEmpty(index) && NotEmpty.stringNotEmpty(type)) {
            return ESUtils.addData(mapObj,index,type);
        }
        resultMap.put("code","4013");
        resultMap.put("msg","mapObj, index, type不能为空");
        return resultMap;

    }
    /** 方法描述
    * @Description: 通过uuid删除数据
    * @Param: [index, type, uuid]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public  Map<String,Object> deleteDataByUUID(String index,String type,String uuid){
        if (NotEmpty.stringNotEmpty(index) && NotEmpty.stringNotEmpty(type) && NotEmpty.stringNotEmpty(uuid)) {
            return ESUtils.deleteDataByUUID(index,type,uuid);
        }
        resultMap.put("code","4014");
        resultMap.put("msg","index, type, uuid不能为空");
        return resultMap;
    }
    /** 方法描述
    * @Description: 通过uuid修改ES中的数据
    * @Param: [index, type, uuid, mapObj]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public  Map<String,Object> updateDataByUUID(String index,String type,String uuid,Map<String,Object> mapObj){
        if (NotEmpty.stringNotEmpty(index) && NotEmpty.stringNotEmpty(type) && NotEmpty.stringNotEmpty(uuid) && NotEmpty.mapNotEmpty(mapObj)) {
            return ESUtils.updateDataByUUID(index,type,uuid,mapObj);
        }
        resultMap.put("code","4014");
        resultMap.put("msg","index, type, uuid, mapObj不能为空");
        return resultMap;

    }

    /** 方法描述
    * @Description: 通过uuid从es中查询数据
    * @Param: [index, type, uuid, fields]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public Map<String,Object> queryDataByUUID(String index,String type,String uuid,String fields){
        if (NotEmpty.stringNotEmpty(index) && NotEmpty.stringNotEmpty(type) && NotEmpty.stringNotEmpty(uuid)) {
            return ESUtils.queryDataByUUID(index,type,uuid,fields);
        }
        resultMap.put("code","4014");
        resultMap.put("msg"," index, type, uuid不能为空");
        return resultMap;

    }
    /** 方法描述
    * @Description: 通过es关键字查询所有集合
    * @Param: [index, type, query, size, fields, sortFields, highListghtField]
    * @return: java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
    * @Author: chj
    * @Date: 2020/3/27
    */
    public  List<Map<String, Object>> queryListData(
            String index, String type, QueryBuilder query, Integer size, String fields,
            String sortFields, String highListghtField){

        List<Map<String,Object>> list = new ArrayList<Map<String, Object>>();
        try {
            List<Map<String, Object>> maps = ESUtils.queryListData(index, type, query, size, fields, sortFields, highListghtField);
            return maps;
        }catch (IndexNotFoundException e){
            resultMap.put("code","4015");
            resultMap.put("msg","没有这个索引");
            list.add(resultMap);
            return list;
        }
    }
}
