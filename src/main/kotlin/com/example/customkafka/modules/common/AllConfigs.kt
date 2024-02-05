package com.example.customkafka.modules.common

data class AllConfigs(
    val replicationFactor: Int,
    val partitions: Int,
    val brokers: List<BrokerConfig>,
    val consumers: Map<Int, List<PartitionData>>,
    val status: ClusterStatus,
)

enum class ClusterStatus {
    GREEN,
    REBALANCING,
    MISSING_BROKERS,
    NO_ZOOKEEPER;
}