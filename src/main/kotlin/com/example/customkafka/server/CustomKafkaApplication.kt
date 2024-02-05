package com.example.customkafka.server

import com.example.customkafka.modules.broker.BrokerConfiguration
import com.example.customkafka.modules.zookeeper.ZookeeperConfiguration
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.openfeign.support.PageJacksonModule
import org.springframework.cloud.openfeign.support.SortJacksonModule
import org.springframework.context.annotation.Import
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
@Import(
    value = [
        BrokerConfiguration::class,
        ZookeeperConfiguration::class
    ]
)
class CustomKafkaApplication

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
