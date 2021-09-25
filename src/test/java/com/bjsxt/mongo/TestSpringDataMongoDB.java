package com.bjsxt.mongo;

import com.bjsxt.mongo.pojo.Order;
import com.mongodb.client.result.UpdateResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;


/**
 * 传统DAO开发,都是基于实体类型+数据访问层代码实现的。
 *
 */

@SpringBootTest(classes = {SpringDataMongoDBApplication.class})
@RunWith(SpringRunner.class)
public class TestSpringDataMongoDB {

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 聚合
     *
     * aggregate(TypedAggregation,Class)
     *  Class , Aggregation
     *
     *  AggregationResult - 聚合结果
     *   results.getUniqueMappedResult();  - 返回聚合后的单行数据，如果聚合结果是多行，抛出异常。
     *   results.getMappedResults();       - 返回聚合后的结果集合，是多行数据。（0-n行)
     */
    @Test
    public void testAgg(){
        TypedAggregation<Order> typedAggregation =
                TypedAggregation.newAggregation(Order.class,
                        Aggregation.match(Criteria.where("payment").gt(120)),
                        Aggregation.group("title").count().as("rows")
//                        Aggregation.skip(1),
//                        Aggregation.limit(1)
                        );
        AggregationResults<Map> results = mongoTemplate.aggregate(typedAggregation, Map.class);
//        results.getUniqueMappedResult();
        List<Map> maps = results.getMappedResults();
        printList(maps);
    }

    /**
     * 分页，  分页 + 排序
     */
    @Test
    public void testPagination(){
       Query query = new Query();
       query.with(
               PageRequest.of(0,3)
       );
       printList(mongoTemplate.find(query,Order.class));
       System.out.println("====================================================");
       Query query1 = new Query();
       query1.with(
             PageRequest.of(0,3,Sort.by(Sort.Direction.ASC,"payment"))
       );
       printList(mongoTemplate.find(query1,Order.class));

    }


    /**
     * 排序
     */
    @Test
    public void testSort(){
        Query query = new Query();
        Sort sort = Sort.by(Sort.Direction.DESC,"title");
        query.with(sort);
        printList(mongoTemplate.find(query,Order.class));
        System.out.println("============================================================");
        Query otherQuery = new Query();
        Sort otherSort = Sort.by(Sort.Order.desc("title"),Sort.Order.asc("payment"));
        otherQuery.with(otherSort);
        printList(mongoTemplate.find(otherQuery,Order.class));
    }

    /**
     * 条件查询
     *   复合条件, and + or
     *   注意嵌套
     */
     @Test
     public void testFindListByAndOr(){
       Criteria and1 = new Criteria().andOperator(
            Criteria.where("title").is("测试新增2"),
            Criteria.where("payment").lt(1000)
       );
         Criteria and2 = new Criteria().andOperator(
                 Criteria.where("title").is("测试新增4"),
                 Criteria.where("payment").lt(1000)
         );
       Query query = Query.query(
               new Criteria().orOperator(
                       and1,and2
               )
       );
       printList(mongoTemplate.find(query,Order.class));

     }


    /**
     * 条件查询
     *  复合条件，or
     *  使用orOperator实现
     */
     @Test
     public void testFindListByOr(){
         Query query =Query.query(
            new Criteria().orOperator(
                    Criteria.where("title").regex("2$"),
                    Criteria.where("payment").lt(2000)
            )
         );
         printList(mongoTemplate.find(query,Order.class));

     }



    /**
     * 条件查询
     *  复合条件，and
     */
    @Test
    public void testFindListByAnd(){
        //混合条件。代表一个字段有多个条件。满足其一即可。 如果是数学范围，则满足全部条件
        // Criteries.where("").gt().lt("")
        Query query1 = Query.query(
          Criteria.where("title").is("测试")
                .regex("2$")
        );
        printList(mongoTemplate.find(query1,Order.class));
        System.out.println("==========================================================");

        //多个条件字段不同,使用andOperations实现
        Query query2 = Query.query(
            new Criteria().andOperator(
                    Criteria.where("title").regex("测试"),
                    Criteria.where("payment").lt(300),
                    Criteria.where("payment").gte(20)
                    )
        );
        printList(mongoTemplate.find(query2,Order.class));

    }


    /**
     * 条件查询
     *  去除重复数据
     *  <T> List<T> findDistinct(Query query,String fieldName,Class pojoClass,Class<T> resultClass)
     *  去重查询,是去除一个字段的重复数据，返回的是去除重复数据后的字段值的集合。类似投影。
     *  投影的是去除重复数据的字段。
     *  fieldName - 投影字段名，去除重复数据的字段名
     *  pojoClass - 访问的集合对应的实体类型，检索Class中的注解或使用默认映射，找集合。
     *  fieldClass - 投影字段类型
     */
     @Test
     public void testFindDistinct(){
        Query query = new Query();
        List<String> result = mongoTemplate.findDistinct(query, "title", Order.class, String.class);
        printList(result);

     }

    /**
     * 条件查询
     * 正则
     */
    @Test
    public void testFindListByRegex(){
        //正则  以'测试'开头的数据。 内容和mongo客户端中的一致。
        // (在mongo客户端中//是标记正则表达式的开始和结束,Java代码中不需要标记正则表达式的范围。)
        Query query = Query.query(Criteria.where("title").regex("^测试"));
        printList(mongoTemplate.find(query,Order.class));
    }


    /**
     * 条件查询
     *  in / not in  / exists
     */
    @Test
    public void testFindListByOther(){
        // in , title in ("测试新增1","测试新增2")
        Query query1 = Query.query(Criteria.where("title").in("测试新增1","测试新增2"));
        printList(mongoTemplate.find(query1,Order.class));

        System.out.println("============================================================");

        // not in ,title not in ("测试新增1","测试新增2")
        Query query2 = Query.query(Criteria.where("title").nin("测试新增1","测试新增2"));
        printList(mongoTemplate.find(query2,Order.class));
        System.out.println("==============================================================");

        //exists , payment exists
        Query query3 = Query.query(Criteria.where("payment").exists(true));
        printList(mongoTemplate.find(query3,Order.class));
        System.out.println("----------------------------------------------------------------");
        query3 = Query.query(Criteria.where("payment").exists(false));
        printList(mongoTemplate.find(query3,Order.class));
        System.out.println("==============================================================");


    }



    /**
     * 条件查询
     *  等值, 不等值
     */
     @Test
     public void testFindListByArgs(){
         //等值查询
         Query query1 = Query.query(
             Criteria.where("payment").is(1)
         );
         List<Order> orders = mongoTemplate.find(query1, Order.class);
         printList(orders);
         System.out.println("================================================");

         //不等值查询， title != "测试新增1"
         Query query2 = Query.query(Criteria.where("title").ne("测试新增1"));
         printList(mongoTemplate.find(query2,Order.class));
         System.out.println("===================================================");

         //不等值查询, 条件范围 100 < payment < 300
         Query query3 = Query.query(Criteria.where("payment").gt(100).lt(300));
         printList(mongoTemplate.find(query3,Order.class));
         System.out.println("=================================================");

         //不等值查询, 范围条件 300 <= payment <= 600
         Query query4 = Query.query(Criteria.where("payment").gte(300).lte(600));
         printList(mongoTemplate.find(query4,Order.class));
         System.out.println("==================================================");
     }


    /**
     * 条件查询 -
     *  无条件查询全部
     */
     @Test
     public void testFindListByNoArgs(){
         List<Order> orders = mongoTemplate.find(new Query(),Order.class);
         printList(orders);
     }

    /**
     * 查询单一对象 - 条件查询
     * <T> T  findOne(Query query,Class<T> clazz)
     * 到clazz对应的集合中，按条件查询数据，并返回第一个匹配成功的数据。
     */
     @Test
     public void testFindFirst(){

        //new Query() - 相当于查询全部数据
        Order order = mongoTemplate.findOne(new Query(),Order.class);

        System.out.println(order);

     }


    /**
     * 查询单一对象 - 主键查询
     * <T> T findById(Object id,Class<T> clazz)
     * 无数据返回null
     */
     @Test
     public void testFindById(){
         Order order = mongoTemplate.findById("614d6aab6ba6e46b1319178e", Order.class);
         System.out.println(order);
     }

    /**
     * 查询 - 全部
     * -  根据类型的注解或默认映射，查询集合全部数据，并封装成List集合，泛型是Class对应类型。
     * findAll(Class clazz,String CollectionName)
     * -  查询collectionName对应的集合，查询结果封装为Class对应类型的对象，并保存在List集合并返回。
     */
    @Test
    public void testFindAll(){
        List<Order> orders = mongoTemplate.findAll(Order.class);
        printList(orders);

        System.out.println("======================================================");
        List<Order> otherorders = mongoTemplate.findAll(Order.class, "order");
        printList(otherorders);

    }

    private void printList(List list){
        for(Object o:list){
            System.out.println(o);
        }
    }


    /**
     * 删除 - 主键删除
     * remove(Object pojo) - 只使用pojo实体对象中的主键作为条件，删除数据
     * 删除 - 全删除
     *   remove(Class clazz) - 直接删除类型对应的集合中的全部数据。 慎用。
     *   remove(Query query) - 根据query条件,删除匹配的数据。注意,直接new Query()作为条件，就是删除全部。
     */
     @Test
     public void testDeleteById(){
         Order o  = new Order();
         o.setId("614d6bbe6ba6e46b13191792");
         o.setTitle("不做任何校验");
         mongoTemplate.remove(o);

         Query query = Query.query(
             Criteria.where("payment").lt(100).gt(30)   //{"$and":[{"payment":{"$gt":30}},{"payment":{"$lt":100}}]}
         );
         mongoTemplate.remove(query,Order.class);

     }




    /**
     * 修改 - 表达式更新
     * $set $inc $push等
     *
     * updateXXX是基于BSON执行的修改操作。不是基于实体对象的修改操作
     *
     * updateFirst  - 相当于 db.collectionName.update({},{},{"multi":false})
     * updateMulti  - 相当于 db.collectionName.update({},{},{"multi":true})
     * UpdateResponse updateFirst(Query query, Object, Class clazz);
     *  根据实体类型中的注解配置或默认映射，实现修改
     *  UpdateResponse updateFirst(Query query, Object, String collectionName);
     *   根据集合中的字段名称，实现修改。
     * Class|String参数，映射的是Query中的条件字段名，Class使用实体中的属性名。String使用集合中的字段名。
     * updateFirst/updateMulti 是基于json运行的，而不是基于对象的。
     */
    @Test
    public void testUpdate(){
       Query query = Query.query( // { title:"o2" }
               Criteria.where("title").is("o2")
       );
       // {"$set":{"title":"更新数据"}},  最常用的更新策略就是$set
       Update update = Update.update("title","更新数据");
       UpdateResult result = mongoTemplate.updateFirst(query, update, Order.class);
       System.out.println("匹配了"+result.getMatchedCount()+"行数据");
       System.out.println("修改了"+result.getModifiedCount()+"行数据");

       System.out.println("==================================================");

       Query multiQuery = new Query();
       Criteria c = Criteria.where("title").in("o4","o3");
       multiQuery.addCriteria(c); // 把匹配条件加入查询逻辑
       Update multiUpdate = new Update();
       multiUpdate.set("title","批量修改");
       UpdateResult multiResult = mongoTemplate.updateMulti(multiQuery,multiUpdate,"order");
       System.out.println("批量修改匹配了"+multiResult.getMatchedCount()+"行数据");
       System.out.println("批量修改更新了"+multiResult.getModifiedCount()+"行数据");
    }

    /**
     * 修改 - 全量替换|覆盖
     * 使用save方法。
     * save方法是用于保存数据的,如果数据不包含主键信息，则一定新增
     * 如果包含主键信息，则匹配检查，相同覆盖，不同新增。
     */
     @Test
     public void testSave(){
        Order o = new Order();
        o.setId("614d6aab6ba6e46b1319178e");
        o.setTitle("测试修改");
        o.setPayment(1);
        o.setItems(Arrays.asList("修改"));
        mongoTemplate.save(o);
     }


    /**新增
     * <T> T insert(T pojo)
     * 使用注解配置或者默认映射机制,实现数据新增。返回新增处理后的对象。如果有自动主键生成，赋值并返回。
     * insert(Collection<T> pojos, Class<T> clazz)
     *  批量新增。且集合中的每个对象的类型都是class类型。对应同一个注解配置或默认映射。
     * <T> T insert(T pojo, String collectionName)
     * 使用注解配置（除@Document外) 或默认映射，向指定的collectionName集合中新增数据。
     *
     */
    @Test
    public void testInsert(){
        Order o1 = new Order();
        o1.setTitle("测试新增1");
        o1.setPayment(100);
        o1.setItems(Arrays.asList("商品1","商品2"));
        System.out.println("新增前:"+o1);
        o1 = mongoTemplate.insert(o1);
        System.out.println("新增后:"+o1);

        Collection<Order> orders = new ArrayList<>();
        for(int i=2;i<5;i++){
            Order o = new Order();
            o.setTitle("测试新增"+i);
            o.setPayment(300+i);
            o.setItems(Arrays.asList("商品"+i));
            orders.add(o);
        }
        System.out.println("批量新增前:"+orders);
        orders = mongoTemplate.insert(orders,Order.class);
        System.out.println("批量新增后:"+orders);
    }


}
