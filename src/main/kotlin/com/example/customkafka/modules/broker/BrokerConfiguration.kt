package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.CommonConfiguration
import jakarta.annotation.PostConstruct
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

private val logger = KotlinLogging.logger {}

@Configuration
@ConditionalOnProperty(prefix = "kafka.broker", name = ["enabled"])
@EnableConfigurationProperties()
@ComponentScan(basePackageClasses = [BrokerConfiguration::class, CommonConfiguration::class])
@EntityScan(
    basePackageClasses = [BrokerConfiguration::class],
)
class BrokerConfiguration(
    val configHandler: ConfigHandler,
    val fileHandler: FileHandler,
    @Value("\${server.address}")
    val address: String,
    @Value("\${server.port}")
    val port: Int
) {

    @PostConstruct
    fun postConstruct() {
        logger.info { "module broker is UP, address: $address, port: $port" }
        while (true){
            try {
                configHandler.start()
                break
            } catch (e: Exception) {
                logger.error { "Error starting broker: $e" }
                Thread.sleep(1000)
            }
        }
        fileHandler.assignPartition()
    }
}