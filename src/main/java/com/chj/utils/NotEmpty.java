package com.chj.utils;

import java.util.List;
import java.util.Map;

/**
 * @author ：chj
 * @date ：Created in 2020/3/27 17:37
 * @params :
 */
public class NotEmpty {
    /** 方法描述
    * @Description: 判断字符串是否为空 true为非空
    * @Param: [s]
    * @return: java.lang.Boolean
    * @Author: chj
    * @Date: 2020/3/27
    */
     public static Boolean stringNotEmpty(String s){
            if (null != s && !"".equals(s)) {
                return true;
            }
            return false;
     }
    /** 方法描述
    * @Description: 判断Integer是否为空 true为非空
    * @Param: [s]
    * @return: java.lang.Boolean
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Boolean integerNotEmpty(Integer i){
        if (null != i && !"".equals(i)) {
            return true;
        }
        return false;
    }
    /** 方法描述
    * @Description: 判断List是否为空 true为非空
    * @Param: [list]
    * @return: java.lang.Boolean
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Boolean listNotEmpty(List<?> list){
        if (null != list && list.size()>0) {
            return true;
        }
        return false;
    }
    /** 方法描述 
    * @Description: 判断map是否为空 true为非空
    * @Param: [map]
    * @return: java.lang.Boolean
    * @Author: chj
    * @Date: 2020/3/27
    */
    public static Boolean mapNotEmpty(Map<?,?> map){
        if (null != map && map.size()>0) {
            return true;
        }
        return false;
    }

}
