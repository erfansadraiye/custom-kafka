package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.*
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestTemplate
import java.io.File

private val logger = KotlinLogging.logger {}

@Service
class ConfigHandler(
    val restTemplate: RestTemplate,
    @Value("\${server.port}")
    val port: Int,
    @Value("\${server.address}")
    val host: String,
    @Value("\${kafka.zookeeper.connect.url}")
    val masterUrl: String,
    @Value("\${kafka.zookeeper.slave.url}")
    val slaveUrl: String,
    ){

    var zookeeperUrl = masterUrl

    lateinit var baseConfig: BaseConfig

    private lateinit var myConfig: MyConfig

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
        var id: Int?
        var dto: RegisterDto
        do {
            dto = callZookeeper("/zookeeper/broker/register", mapOf("host" to host, "port" to port), RegisterDto::class.java)!!
            if (dto.clearDirectory) {
                logger.debug { "deleting files... ${File("data").listFiles()!!.map { it.name }}" }
                for (file in File("data").listFiles()!!) {
                    file.delete()
                }
            }
            id = dto.id
            Thread.sleep(1000)
        } while (id == null)
        logger.debug { "Registered with id: $id" }
        val config = dto.allConfigs
        logger.debug { "Got config: $config" }
        val myBaseConfig = config!!.brokers.find { it.brokerId == id }!!
        baseConfig = BaseConfig(id, config.replicationFactor, config.partitions, myBaseConfig)
        myConfig = myBaseConfig.config!!
        otherBrokers = config.brokers.filter { it.brokerId != id }
        status = config.status
    }

    fun reload(registerDto: RegisterDto? = null) {
        val id = baseConfig.brokerId
        logger.debug { "Reloading config with id $id" }
        val config = registerDto?.allConfigs ?: callZookeeper("/zookeeper/config", null, AllConfigs::class.java)
        logger.debug { "Got config: $config" }
        val myBaseConfig = config!!.brokers.find { it.brokerId == id }!!
        baseConfig = BaseConfig(id, config.replicationFactor, config.partitions, myBaseConfig)
        myConfig = myBaseConfig.config!!
        otherBrokers = config.brokers.filter { it.brokerId != id }
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
        val response = callZookeeper("/zookeeper/partition/$id", null, PartitionDto::class.java)
        return response!!
    }

    fun<T> callZookeeper(path: String, content: Any?, clazz: Class<T>): T? {
        while (true) {
            try {
                return restTemplate.postForEntity("$zookeeperUrl$path", content, clazz).body
            } catch (e: ResourceAccessException) {
                logger.warn { "Zookeeper Node failing..." }
                if (zookeeperUrl == slaveUrl) logger.error { "Both Zookeepers are failing!" }
                zookeeperUrl = slaveUrl
            }
        }
    }
}

data class BaseConfig(
    val brokerId: Int,
    val replicationFactor: Int,
    val partitions: Int,
    val brokerConfig: BrokerConfig
)

