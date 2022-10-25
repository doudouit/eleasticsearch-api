package com.allen.es;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.allen.es.mapper")
public class ElasticsearchApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApiApplication.class, args);
    }

}
