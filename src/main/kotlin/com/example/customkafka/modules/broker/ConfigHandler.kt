package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.AllConfigs
import com.example.customkafka.modules.common.BrokerConfig
import com.example.customkafka.modules.common.MyConfig
import com.example.customkafka.modules.common.PartitionData
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.util.LinkedList
import java.util.Queue

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

    private lateinit var consumerPartitionQueue: Map<Int, Queue<PartitionData>>

    private lateinit var partitions: Map<Int, PartitionData>

    fun findReplicaBrokerIds(partition: Int): List<Int> {
        return otherBrokers.filter { it.config!!.replicaPartitionList.contains(partition) }.map { it.brokerId!! }
    }

    fun findLeaderBrokerId(partition: Int): Int {
        return otherBrokers.find { it.config!!.leaderPartitionList.contains(partition) }!!.brokerId!!
    }

    fun getBrokerConfig(brokerId: Int): BrokerConfig {
        return otherBrokers.find { it.brokerId == brokerId }!!
    }

    fun getMyLogDir() = "data/broker-" + baseConfig.brokerId

    fun start() {
        val id = restTemplate.postForEntity(
            "$zookeeperUrl/zookeeper/register",
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
        consumerPartitionQueue = config.consumers.mapValues { LinkedList(it.value) }
        partitions = config.consumers.values.flatten().associateBy { it.id }
    }

    fun getPartition(key: String): PartitionData {
        val id = ((key.hashCode() % baseConfig.partitions) + baseConfig.partitions) % baseConfig.partitions
        return partitions[id]!!
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

    fun getPartitionForConsumer(id: Int): PartitionData? {
        val partition = consumerPartitionQueue[id]?.poll() ?: throw Exception("unregistered consumer")
        consumerPartitionQueue[id]!!.add(partition)
        return partition
    }

    fun registerConsumer(): Int {
        val id = restTemplate.postForEntity("$zookeeperUrl/consumer/register", null, String::class.java).body
        return id!!.toInt()
    }
}

data class BaseConfig(
    val brokerId: Int,
    val replicationFactor: Int,
    val partitions: Int,
    val brokerConfig: BrokerConfig
)

