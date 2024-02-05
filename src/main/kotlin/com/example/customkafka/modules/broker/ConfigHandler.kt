package com.example.customkafka.modules.broker

import mu.KotlinLogging
import org.springframework.stereotype.Service

private val logger = KotlinLogging.logger {}

@Service
class ConfigHandler {

    // TODO: Get the config from zookeeper
    private val myConfig: MyConfig = MyConfig(listOf(0, 1), listOf(2, 3))

    private val baseConfig: BaseConfig = BaseConfig(0, "logDir", 2, 4)

    private val otherBrokers: List<BrokerConfig> = listOf(
        BrokerConfig(1, "localhost", 8081, MyConfig(listOf(2, 3), listOf(0, 1)))
    )

    fun findReplicaBrokerIds(partition: Int): List<Int> {
        return otherBrokers.filter { it.config.replicaPartitionList.contains(partition) }.map { it.brokerId }
    }

    fun findLeaderBrokerId(partition: Int): Int {
        return otherBrokers.find { it.config.leaderPartitionList.contains(partition) }!!.brokerId
    }

    fun init() {
        logger.info { "When the borker started first time" }
        // TODO: Give the config from zookeeper and save them in disk
    }

    fun start() {
        logger.info { "When the borker started" }
        //TODO : check the config from disk and start the broker, if not found then call init
        //TODO : get status of all the brokers and update the config

    }

    fun getPartitionNumber(key: String): Int {
        return ((key.hashCode() % baseConfig.partitions) + baseConfig.partitions) % baseConfig.partitions
    }

    fun getLeaderPartitionList(): List<Int> {
        return myConfig.leaderPartitionList
    }

    fun amILeader(partition: Int): Boolean {
        return myConfig.leaderPartitionList.contains(partition)
    }

    fun amIReplica(partition: Int): Boolean {
        return myConfig.replicaPartitionList.contains(partition)
    }

    fun getReplicaPartitionList(): List<Int> {
        return myConfig.replicaPartitionList
    }

}

// get from zookeeper and save in disk
data class BaseConfig(
    val brokerId: Int,
    val logDir: String,
    val replicationFactor: Int,
    val partitions: Int
)

// get from zookeeper when restart the broker
data class MyConfig(
    val leaderPartitionList: List<Int>,
    val replicaPartitionList: List<Int>
)

data class BrokerConfig(
    val brokerId: Int,
    val address: String,
    val port: Int,
    val config: MyConfig
)

