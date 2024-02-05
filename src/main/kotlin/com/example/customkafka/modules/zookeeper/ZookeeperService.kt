package com.example.customkafka.modules.zookeeper

import com.example.customkafka.modules.common.*
import com.example.customkafka.modules.common.AllConfigs
import com.example.customkafka.server.objectMapper
import jakarta.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.io.File


@Service
class ZookeeperService(
    @Value("\${kafka.brokers.num}")
    val brokerCount: Int,
    @Value("\${kafka.partitions.num}")
    val partitionCount: Int,
    @Value("\${kafka.replication-factor}")
    val replicationFactor: Int,
    val restTemplate: RestTemplate,
) {
    //TODO need to load from config
    val config = ZookeeperConfig(true)

    var leaders = mapOf<Int, MutableList<Int>>()
    var replications = mapOf<Int, MutableList<Int>>()

    var brokers = mutableListOf<BrokerConfig>()

    @PostConstruct
    fun setup() {
        if (!config.isMaster) return
        doPartitionConfigs()
        doBrokersConfigs()
    }

    private fun doBrokersConfigs() {
        val file = File("zookeeper/zookeeperBrokers.txt")
        if (file.exists()) {
            brokers = if (file.length() != 0L)
                objectMapper.readValue(file.readText(), AllBrokers::class.java).brokers.toMutableList()
            else
                mutableListOf()
        } else {
            file.parentFile.mkdirs()
            file.createNewFile()
        }
    }

    private fun doPartitionConfigs() {
        val file = File("zookeeper/zookeeperPartitions.txt")
        if (file.exists()) {
            val configs = objectMapper.readValue(file.readText(), PartitionConfig::class.java)
            leaders = configs.leaderPartitionList
            replications = configs.replicaPartitionList
        } else {
            val partitions = 0 until partitionCount
            leaders = (0 until brokerCount).associateWith { mutableListOf() }
            replications = (0 until brokerCount).associateWith { mutableListOf() }
            partitions.forEachIndexed { index, p ->
                leaders[index % brokerCount]!!.add(p)
            }
            partitions.forEachIndexed { index, p ->
                val availableBrokers = leaders.filter { !it.value.contains(p) }.map { it.key }
                if (availableBrokers.isEmpty()) throw Exception("Replication factor cannot be bigger than broker count!")
                repeat(replicationFactor) {
                    replications[availableBrokers[it]]!!.add(p)
                }
            }
            val text = objectMapper.writeValueAsString(PartitionConfig(leaders, replications))
            //TODO inform slave zookeeper about new file
            file.parentFile.mkdirs()
            file.createNewFile()
            file.writeText(text)
        }
    }


    fun getConfigs(): AllConfigs {
        return AllConfigs(
            replicationFactor,
            partitionCount,
            brokers
        )
    }

    fun registerBroker(host: String, port: Int): Int {
        brokers.find { it.host == host && it.port == port }?.let {
            return it.brokerId
        }
        val id = (brokers.lastOrNull()?.brokerId ?: -1) + 1
        val config = BrokerConfig(id, host, port, MyConfig(leaders[id]!!, replications[id]!!))
        brokers.forEach {
            restTemplate.postForEntity(
                "http://${it.host}:${it.port}/config/reload",
                null,
                String::class.java
            )
        }
        brokers.add(config)
        val file = File("zookeeper/zookeeperBrokers.txt")
        file.writeText(objectMapper.writeValueAsString(AllBrokers(brokers)))
        return id
    }

}

data class AllBrokers(
    val brokers: List<BrokerConfig>
)