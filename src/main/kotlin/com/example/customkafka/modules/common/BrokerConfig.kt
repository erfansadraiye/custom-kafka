package com.example.customkafka.modules.common

data class BrokerConfig(
    val brokerId: Int,
    val host: String,
    val port: Int,
    val config: MyConfig
)