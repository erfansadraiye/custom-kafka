package com.example.customkafka.modules.zookeeper

data class ConsumerConfig(
    val consumers: Map<Int, MutableList<Int>> = mapOf()
)