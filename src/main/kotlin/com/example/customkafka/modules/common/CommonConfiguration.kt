package com.example.customkafka.modules.common

import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.cloud.openfeign.EnableFeignClients
import org.springframework.context.annotation.Configuration

//@EnableFeignClients
@Configuration
@EnableConfigurationProperties()
@EntityScan
class CommonConfiguration {
}