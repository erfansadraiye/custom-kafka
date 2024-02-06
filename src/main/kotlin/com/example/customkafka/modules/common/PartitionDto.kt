package com.example.customkafka.modules.common

data class PartitionDto(
    val partitionId: Int? = null,
    val offset: Long? = null,
)
