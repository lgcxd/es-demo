package com.employee;

import com.sun.xml.internal.ws.api.config.management.policy.ManagementAssertion;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @ClassName EmployeeCRUDApp
 * @Author: ChenBJ
 * @Description: 增删改查的应用程序
 * @Date: 2018/12/28 11:37
 * @Version:
 */
public class EmployeeCRUDApp {
    @SuppressWarnings({"unchecked","resource"})
    public static void main(String[] args) throws IOException {
        //先构建client
        Settings  settings = Settings.builder()
                .put("cluster.name","elasticsearch")
                .build();

            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
            //createEmployee(client);//创建一行记录
           // updateEmployee(client);//修改记录
            deleteEmployee(client); //删除员工信息
            client.close();
    }
    /**
     * @Author: ChenBJ
     * @Description:  创建员工信息(创建一个document)
     * @Date: 2018/12/28 11:46
     * @Param: @param null
     * @return:
     */
    private static void createEmployee(TransportClient client) throws IOException {
        IndexResponse response = client.prepareIndex("company","employee","1")
                .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("name","jack")
                        .field("age",27)
                        .field("position","technique")
                        .field("country","china")
                        .field("join_date","2018-01-01")
                        .field("salary",10000)
                        .endObject()).get();
        System.out.println("返回结果为:"+response.getResult());
    }

    /**
     * @Author: ChenBJ
     * @Description: 修改员工信息
     * @Date: 2018/12/28 13:17
     * @Param: @param null
     * @return:
     */
    private static void updateEmployee(TransportClient client) throws IOException {
        UpdateResponse response = client.prepareUpdate("company","employee","1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("position","technique manager")
                        .endObject()).get();
        System.out.println("返回的结果为"+response.getResult());
    }

    /**
     * @Author: ChenBJ
     * @Description: 删除员工信息
     * @Date: 2018/12/28 13:24
     * @Param: @param null
     * @return: 
     */
    private static void deleteEmployee(TransportClient client){
        DeleteResponse response = client.prepareDelete("company","employee","1")
                .get();
        System.out.println("返回结果为:"+response.getResult());

    }
}
