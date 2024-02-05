package com.example.customkafka.modules.common

data class BrokerConfig(
    val brokerId: Int? = null,
    val host: String? = null,
    val port: Int? = null,
    val config: MyConfig? = null
)