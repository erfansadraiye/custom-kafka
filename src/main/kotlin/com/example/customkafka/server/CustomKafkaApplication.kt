package com.example.customkafka.server

import com.example.customkafka.modules.broker.BrokerConfiguration
import com.example.customkafka.modules.zookeeper.ZookeeperConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
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
