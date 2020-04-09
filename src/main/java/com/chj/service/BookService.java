package com.chj.service;

import com.chj.mapper.BookMapper;
import com.chj.model.Book;
import com.chj.utils.ESUtils;
import com.chj.utils.NotEmpty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.chj.properties.ESStaticProperties.*;

/**
 * @author ：chj
 * @date ：Created in 2020/3/27 16:31
 * @params :
 */
@Service
public class BookService {
    @Autowired
    private BookMapper bookMapper;
    public static Map<String,Object> resultMap = new ConcurrentHashMap<String, Object>();
    /** 方法描述
     * @Description: 查询Book数据
     * @Param: []
     * @return: java.util.Map<java.lang.String,java.lang.Object>
     * @Author: chj
     * @Date: 2020/3/27
     */
    public Map<String,Object> selectAll(){
        List<Book> books = null;
        books = bookMapper.selectAll();
        if (NotEmpty.listNotEmpty(books)) {
            for (Book book:books){
                Map<String, Object> objectMap = ESUtils.objectTurnMap(book);
                ESUtils.addData(objectMap, INDEX_TEST, TYPE_TEST);
            }
            resultMap.put("code","2050");
            resultMap.put("msg","查询book添加成功");
        }else {
            resultMap.put("code","4030");
            resultMap.put("msg","查询book添加失败");
        }
        return resultMap;
    }

}
