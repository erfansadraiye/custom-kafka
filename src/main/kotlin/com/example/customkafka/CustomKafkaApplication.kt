package com.example.customkafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CustomKafkaApplication

fun main(args: Array<String>) {
    runApplication<CustomKafkaApplication>(*args)
}
