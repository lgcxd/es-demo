package com.employee;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;



/**
 * @ClassName EmployeeAggrApp
 * @Author: ChenBJ
 * @Description: 员工聚合分析应用程序
 * @Date: 2018/12/28 13:56
 * @Version:
 */
public class EmployeeAggrApp {
    @SuppressWarnings({ "unchecked", "resource" })
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));

        //聚合分析
        /**
         * （1）首先按照country国家来进行分组
         （2）然后在每个country分组内，再按照入职年限进行分组
         （3）最后计算每个分组内的平均薪资*/
        SearchResponse response = client.prepareSearch("company")
                .addAggregation(AggregationBuilders.terms("group_by_country").field("country")
                        .subAggregation(AggregationBuilders
                                .dateHistogram("group_by_join_date").field("join_date")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_salary").field("salary"))
                        )
                )
                .execute().actionGet();

        Map<String, Aggregation> aggrMap = response.getAggregations().asMap();

        StringTerms country = (StringTerms) aggrMap.get("group_by_country");
        Iterator<Bucket> countryIterator = country.getBuckets().iterator();
        while(countryIterator.hasNext()) {
            Bucket groupByCountryBucket = countryIterator.next();
            System.out.println(groupByCountryBucket.getKey() + ":" + groupByCountryBucket.getDocCount());

            Histogram groupByJoinDate = (Histogram) groupByCountryBucket.getAggregations().asMap().get("group_by_join_date");
            Iterator<org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket> groupByJoinDateBucketIterator = groupByJoinDate.getBuckets().iterator();
            while(groupByJoinDateBucketIterator.hasNext()) {
                org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket groupByJoinDateBucket = groupByJoinDateBucketIterator.next();
                System.out.println(groupByJoinDateBucket.getKey() + ":" +groupByJoinDateBucket.getDocCount());

                Avg avg = (Avg) groupByJoinDateBucket.getAggregations().asMap().get("avg_salary");
                System.out.println(avg.getValue());
            }
        }

        client.close();
    }
}
