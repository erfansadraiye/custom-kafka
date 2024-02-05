package com.example.customkafka.modules.zookeeper

import com.example.customkafka.modules.common.CommonConfiguration
import jakarta.annotation.PostConstruct
import mu.KotlinLogging
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.cloud.openfeign.EnableFeignClients
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

private val log = KotlinLogging.logger {}

//@EnableFeignClients
@Configuration
@ConditionalOnProperty(prefix = "ir.custom-kafka.zookeeper", name = ["enabled"])
@EnableConfigurationProperties()
@ComponentScan(basePackageClasses = [ZookeeperConfiguration::class, CommonConfiguration::class])
@EntityScan(
    basePackageClasses = [ZookeeperConfiguration::class],
)
class ZookeeperConfiguration {

    @PostConstruct
    fun postConstruct() {
        log.info { "module zookeeper is UP" }
    }
}