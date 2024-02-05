package com.example.customkafka.modules.common

// get from zookeeper when restart the broker
data class MyConfig(
    val leaderPartitionList: List<Int>,
    val replicaPartitionList: List<Int>
)