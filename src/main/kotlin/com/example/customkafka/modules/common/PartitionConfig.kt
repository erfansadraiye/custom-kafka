package com.example.customkafka.modules.common

data class PartitionConfig(
    val leaderPartitionList: Map<Int, MutableList<Int>>,
    val replicaPartitionList: Map<Int, MutableList<Int>>
)
