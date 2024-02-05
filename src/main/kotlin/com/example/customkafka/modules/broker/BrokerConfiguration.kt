package com.example.customkafka.modules.broker

import com.example.customkafka.modules.common.CommonConfiguration
import jakarta.annotation.PostConstruct
import mu.KotlinLogging
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
class BrokerConfiguration {

    @PostConstruct
    fun postConstruct() {
        logger.info { "module broker is UP" }
    }
}