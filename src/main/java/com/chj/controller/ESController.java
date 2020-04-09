package com.chj.controller;

import com.chj.service.BookService;
import com.chj.service.ESService;
import org.elasticsearch.index.query.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

import static com.chj.properties.ESStaticProperties.*;

/**
 * @author ：chj
 * @date ：Created in 2020/3/27 16:15
 * @params :
 */
@RequestMapping("/es")
@RestController
public class ESController {
    @Autowired
    private ESService esService;

    @Autowired
    private BookService bookService;
    /** 方法描述
    * @Description: 向es中添加一个index
    * @Param: [index]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    @PostMapping("/createIndex")
    public Map<String,Object> createIndex(String index){
        System.out.println(index);
        return esService.createIndex(index);
    }
    /** 方法描述
    * @Description: 删除index
    * @Param: [index]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    @PostMapping("/deleteIndex")
    public  Map<String,Object> deleteIndex(String index){
        System.out.println(index);
        return esService.deleteIndex(index);
    }


    /** 方法描述
    * @Description: 向ES中添加一个数据
    * @Param: [mapObj, index, type, uuid]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    @PostMapping("/addData")
    public Map<String,Object> addData(@RequestParam Map<String,Object> mapObj, String uuid){
        System.out.println(mapObj);
        System.out.println(uuid);
        return esService.addData(mapObj,INDEX_TEST,TYPE_TEST,uuid);
    }
    /** 方法描述
    * @Description: 从mysql中查询Book集合，添加到ES中
    * @Param: [index, type, uuid]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    @PostMapping("/BookAddData")
    public Map<String,Object> BookAddData(){
        return bookService.selectAll();
    }
    /** 方法描述
    * @Description: .通过uuid从ES中查询一条数据
    * @Param: [index, type, uuid, fields]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    @PostMapping("/queryDataByUUID")
    public Map<String,Object> queryDataByUUID(String fields){
        System.out.println(fields);
        return esService.queryDataByUUID(INDEX_TEST,TYPE_TEST, "123456",fields);
    }
    /** 方法描述
    * @Description: 实现指定type的所有数据
    * @Param: [index, type, uuid, fields]
    * @return: java.util.Map<java.lang.String,java.lang.Object>
    * @Author: chj
    * @Date: 2020/3/27
    */
    @PostMapping("/queryDataByType")
    public Map<String,Object> queryDataByType(String fields){
        System.out.println(fields);
        return  esService.queryDataByUUID(INDEX_TEST,TYPE_TEST, "123456",fields);
    }
    /** 方法描述
    * @Description: 查询
    * @Param: [index, type, query, size, fields, sortFields, highListghtField]
    * @return: java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
    * @Author: chj
    * @Date: 2020/3/27
    */
    @PostMapping("/all")
    public List<Map<String, Object>> queryListData(){
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        BoolQueryBuilder must = queryBuilder.must(matchAllQueryBuilder);
        return esService.queryListData(INDEX_TEST,TYPE_TEST, must, 100, null, null, null);
    }

    @PostMapping("/selectAllLike")
    public List<Map<String, Object>> selectAllLike(){
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("bookName", "iPhone");
        BoolQueryBuilder must = queryBuilder.must(matchPhraseQueryBuilder);
        return esService.queryListData(INDEX_TEST,TYPE_TEST, must, 100, null, null, null);
    }
}
