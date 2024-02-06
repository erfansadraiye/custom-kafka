package com.example.customkafka.modules.common

// get from zookeeper when restart the broker
data class MyConfig(
    val leaderPartitionList: MutableList<Int> = mutableListOf(),
    val replicaPartitionList: MutableList<Int> = mutableListOf()
)

data class PartitionData(
    val id: Int,
    var lastOffset: Long,
    var lastCommit: Long,
)