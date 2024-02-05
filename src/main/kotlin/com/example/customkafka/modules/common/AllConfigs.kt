package com.example.customkafka.modules.common

data class AllConfigs(
    val replicationFactor: Int,
    val partitions: Int,
    val brokers: List<BrokerConfig>
)