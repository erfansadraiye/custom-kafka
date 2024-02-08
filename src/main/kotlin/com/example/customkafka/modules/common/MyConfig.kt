package com.example.customkafka.modules.common

import java.util.*

// get from zookeeper when restart the broker
data class MyConfig(
    val leaderPartitionList: MutableList<Int> = mutableListOf(),
    val replicaPartitionList: MutableList<Int> = mutableListOf()
)

data class PartitionData(
    val id: Int? = null,
    var lastOffset: Long? = null,
    var lastCommit: Long? = null,
    var timestamp: Date? = null,
): Comparable<PartitionData> {

    override fun compareTo(other: PartitionData): Int {
        return timestamp!!.compareTo(other.timestamp)
    }

}