package com.employee;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;

/**
 * @ClassName EmployeeSearchApp
 * @Author: ChenBJ
 * @Description: 员工搜索应用程序
 * @Date: 2018/12/28 13:29
 * @Version:
 */
public class EmployeeSearchApp {
    @SuppressWarnings({"unchecked","resource"})
    public static void main(String[] args) throws IOException {
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
        prepareData(client);//新增员工信息
        //searchEmployee(client); //查询30-40员工并且是技术的员工信息一条数据分页
        client.close();
    }
    /**
     * @Author: ChenBJ
     * @Description: 查询
     * @Date: 2018/12/28 13:42
     * @Param: @param null
     * @return:
     */
    private static void searchEmployee(TransportClient client){
        //（1）搜索职位中包含technique的员工
        //（2）同时要求age在30到40岁之间
        //（3）分页查询，查找第一页
        SearchResponse response = client.prepareSearch("company")
                .setTypes("employee")
                .setQuery(QueryBuilders.matchQuery("position","technique"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(40))
                .setFrom(0).setSize(1)
                .get();
        SearchHit[] searchHits = response.getHits().getHits();
        for (int i = 0; i<searchHits.length; i++){
            System.out.println(searchHits[i].getSourceAsString());
        }
    }

    /**
     * @Author: ChenBJ
     * @Description: 写入员工信息
     * @Date: 2018/12/28 13:34
     * @Param: @param null
     * @return:
     */
    private static void prepareData(TransportClient client) throws IOException {
        client.prepareIndex("company","employee","1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2018-01-01")
                        .field("salary", 1000)
                        .endObject()
                ).get();

        client.prepareIndex("company","employee","2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "marry")
                        .field("age", 35)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 12000)
                        .endObject()
                ).get();

        client.prepareIndex("company","employee","3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "tom")
                        .field("age", 32)
                        .field("position", "senior technique software")
                        .field("country", "china")
                        .field("join_date", "2016-01-01")
                        .field("salary", 11000)
                        .endObject()
                ).get();

        client.prepareIndex("company","employee","4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jen")
                        .field("age", 25)
                        .field("position", "junior finance")
                        .field("country", "usa")
                        .field("join_date", "2017-01-01")
                        .field("salary", 7000)
                        .endObject()
                ).get();

        client.prepareIndex("company","employee","5")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "mike")
                        .field("age", 37)
                        .field("position", "finance manager")
                        .field("country", "usa")
                        .field("join_date", "2015-01-01")
                        .field("salary", 15000)
                        .endObject()
                ).get();
    }
}
