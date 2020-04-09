package com.chj.mapper;

import com.chj.model.Book;
import org.apache.ibatis.annotations.Mapper;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Author: chj
 * @Date: 2020/3/27
 */
@Mapper
public interface BookMapper {

    /** 方法描述
    * @Description: 根据id删除
    * @Param: [id]
    * @return: int
    * @Author: chj
    * @Date: 2020/3/27
    */
    int deleteByPrimaryKey(Integer id);

    /** 方法描述
    * @Description: 根据实体修改
    * @Param: [record]
    * @return: int
    * @Author: chj
    * @Date: 2020/3/27
    */
    int insert(Book record);

    /** 方法描述
    * @Description: 跟俊id查询
    * @Param: [id]
    * @return: com.chj.model.Book
    * @Author: chj
    * @Date: 2020/3/27
    */
    Book selectByPrimaryKey(Integer id);
    /** 方法描述
    * @Description: 查询所有
    * @Param: []
    * @return: java.util.List<com.chj.model.Book>
    * @Author: chj
    * @Date: 2020/3/27
    */
    List<Book> selectAll();

    /** 方法描述
    * @Description: 根据实体修改
    * @Param: [record]
    * @return: int
    * @Author: chj
    * @Date: 2020/3/27
    */
    int updateByPrimaryKey(Book record);

}