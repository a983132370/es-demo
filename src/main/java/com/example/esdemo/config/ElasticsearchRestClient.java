package com.example.esdemo.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Objects;

@Configuration
@EnableConfigurationProperties(value = RestClientProperties.class)
public class ElasticsearchRestClient {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final int ADDRESS_LENGTH = 2;
    private static final String HTTP_SCHEME = "http";

    /**
     * 使用冒号隔开ip和端口1
     */
    @Autowired
    RestClientProperties restClientProperties;

    @Bean
    public RestClientBuilder restClientBuilder() {
        HttpHost[] hosts = restClientProperties.getUris().stream()
                .map(uri -> {
                    assert StringUtils.isNotEmpty(uri);
                    String[] address = uri.replace("http://","").split(":");
                    if (address.length == ADDRESS_LENGTH) {
                        String ip = address[0];
                        int port = Integer.parseInt(address[1]);
                        return new HttpHost(ip, port);
                    } else {
                        return null;
                    }
                })//将uri转换成 HttpHost对象
                .filter(Objects::nonNull)//过滤掉空对象
                .toArray(HttpHost[]::new);//返回HttpHost数组

        log.debug("hosts:{}", Arrays.toString(hosts));
        return RestClient.builder(hosts);
    }

    @Bean(name = "highLevelClient")
    public RestHighLevelClient esClient(@Autowired RestClientBuilder restClientBuilder) {
        restClientBuilder.setMaxRetryTimeoutMillis(60000);
        return new RestHighLevelClient(restClientBuilder);
    }
}