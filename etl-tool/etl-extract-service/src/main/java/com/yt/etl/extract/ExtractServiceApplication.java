package com.yt.etl.extract;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ExtractServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExtractServiceApplication.class, args);
    }

}
