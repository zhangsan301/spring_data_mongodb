package com.bjsxt.mongo.pojo;


/**
 * 定义实体类型
 * 对应MongoDB中的order集合
 * 集合命名是: order
 *
 */

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

/**
 * Document注解:  描述当前类型和MongoDB中的一个集合对应
 * 可以省略注解。
 * 属性:
 * value - 对应MongoDB中的集合命名。默认命名规则是，类名首字母转小写
 * Spring Data MongoDB对数据和实体类型的映射处理，是基于Property的。
 * 需要为实体类型提供getter/setter方法
 */
@Document("order")
@Data
public class Order {
    /**
     * 主键字段
     * 可以使用注解Id描述。代表此字段是主键字段
     * 如果属性命名是： id / _id，注解可以省略
     *
     * MongoDB中，默认的主键类型是ObjectId,对应Java中的类型是String/ObjectId(是Bson包中的。)
     * 如果Java实体中使用字符串类型作为主键，映射的MongoDB默认主键方式是。
     * MongoDB -  ObjectId("xxxxxxxx")
     * Java对象 - "xxxxxxxx"
     */
    @Id
    private  String id;
    /**
     * 可以使用注解Field描述属性。用于配置Java实体属性和MongoDB集合Field字段的映射关系。
     * 默认映射关系是同名映射。
     */
    @Field("title")
    private String title;
    private double payment;
    private List<String> items;



}
