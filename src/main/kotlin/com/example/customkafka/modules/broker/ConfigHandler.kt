package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.*
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate

private val logger = KotlinLogging.logger {}

@Service
class ConfigHandler(
    val restTemplate: RestTemplate,
    @Value("\${kafka.zookeeper.connect.url}")
    val zookeeperUrl: String,
    @Value("\${server.port}")
    val port: Int,
    @Value("\${server.address}")
    val host: String,
    ){

    private lateinit var myConfig: MyConfig

    private lateinit var baseConfig: BaseConfig

    private lateinit var otherBrokers: List<BrokerConfig>

    lateinit var status: ClusterStatus

    fun findReplicaBrokerIds(partition: Int): List<Int> {
        return otherBrokers.filter { it.config!!.replicaPartitionList.contains(partition) }.map { it.brokerId!! }
    }

    fun findLeaderBrokerId(partition: Int): Int {
        logger.debug { "otherBrokers: $otherBrokers" }
        return otherBrokers.find { it.config!!.leaderPartitionList.contains(partition) }!!.brokerId!!
    }

    fun getBrokerConfig(brokerId: Int): BrokerConfig {
        return otherBrokers.find { it.brokerId == brokerId }!!
    }

    fun getMyLogDir() = "data"

    fun start() {
        val id = restTemplate.postForEntity(
            "$zookeeperUrl/zookeeper/broker/register",
            mapOf("host" to host, "port" to port),
            String::class.java
        ).body
        logger.debug { "Registered with id: $id" }
        val config = restTemplate.postForEntity("$zookeeperUrl/zookeeper/config", null, AllConfigs::class.java).body
        logger.debug { "Got config: $config" }
        val myBaseConfig = config!!.brokers.find { it.brokerId == id!!.toInt() }!!
        baseConfig = BaseConfig(id!!.toInt(), config.replicationFactor, config.partitions, myBaseConfig)
        myConfig = myBaseConfig.config!!
        otherBrokers = config.brokers.filter { it.brokerId != id.toInt() }
        status = config.status
    }

    fun getPartition(key: String): Int {
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

    fun getPartitionForConsumer(id: Int): PartitionDto {
        val response = restTemplate.postForEntity("$zookeeperUrl/zookeeper/partition/$id", null, PartitionDto::class.java).body
        if (response!!.partitionId == null) throw Exception("Not registered consumer.")
        return response
    }
}

data class BaseConfig(
    val brokerId: Int,
    val replicationFactor: Int,
    val partitions: Int,
    val brokerConfig: BrokerConfig
)

