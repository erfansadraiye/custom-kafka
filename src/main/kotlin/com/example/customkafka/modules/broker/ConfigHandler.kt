package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.AllConfigs
import com.example.customkafka.modules.common.BrokerConfig
import com.example.customkafka.modules.common.MyConfig
import jakarta.annotation.PostConstruct
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate

private val logger = KotlinLogging.logger {}

@Service
class ConfigHandler(
    val restTemplate: RestTemplate,
    @Value("\${kafka.zookeeper.connect.url}")
    val zookeeperUrl: String
){

    // TODO: Get the config from zookeeper
    private lateinit var myConfig: MyConfig

    private lateinit var baseConfig: BaseConfig

    private lateinit var otherBrokers: List<BrokerConfig>

    fun findReplicaBrokerIds(partition: Int): List<Int> {
        return otherBrokers.filter { it.config.replicaPartitionList.contains(partition) }.map { it.brokerId }
    }

    fun findLeaderBrokerId(partition: Int): Int {
        return otherBrokers.find { it.config.leaderPartitionList.contains(partition) }!!.brokerId
    }

    fun getBrokerConfig(brokerId: Int): BrokerConfig {
        return otherBrokers.find { it.brokerId == brokerId }!!
    }

    fun getMyLogDir() = "logDir/broker-" + baseConfig.brokerId

    @PostConstruct
    fun start() {
        val id = restTemplate.postForEntity(zookeeperUrl + "/register", mapOf("host" to "localhost", "port" to 8080), String::class.java).body
        logger.debug { "Registered with id: $id" }
        val config = restTemplate.getForEntity(zookeeperUrl + "/config", AllConfigs::class.java).body
        logger.debug { "Got config: $config" }
        myConfig = config!!.brokers.find { it.brokerId == id!!.toInt() }!!.config
        baseConfig = BaseConfig(id!!.toInt(), config.replicationFactor, config.partitions)
        otherBrokers = config.brokers.filter { it.brokerId != id.toInt() }
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

data class BaseConfig(
    val brokerId: Int,
    val replicationFactor: Int,
    val partitions: Int
)

