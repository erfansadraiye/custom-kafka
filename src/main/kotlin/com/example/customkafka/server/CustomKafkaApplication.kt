package com.example.customkafka.server

import com.example.customkafka.modules.broker.BrokerConfiguration
import com.example.customkafka.modules.zookeeper.ZookeeperConfiguration
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.util.StdDateFormat
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.web.client.RestTemplate
import java.time.Duration

@EnableScheduling
@SpringBootApplication
@Import(
    value = [
        BrokerConfiguration::class,
        ZookeeperConfiguration::class
    ]
)
class CustomKafkaApplication{

    @Bean
    fun restTemplate(): RestTemplate {
        return RestTemplateBuilder()
            .setConnectTimeout(Duration.ofSeconds(5))
            .setReadTimeout(Duration.ofSeconds(5))
            .build()
    }
}

fun main(args: Array<String>) {
    runApplication<CustomKafkaApplication>(*args)
}


val objectMapper: JsonMapper = JsonMapper.builder().configure(JsonParser.Feature.ALLOW_COMMENTS, true)
    .serializationInclusion(JsonInclude.Include.NON_NULL)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .defaultDateFormat(StdDateFormat())
//    .addModules(JavaTimeModule(), KotlinModule(),)
    .build()
