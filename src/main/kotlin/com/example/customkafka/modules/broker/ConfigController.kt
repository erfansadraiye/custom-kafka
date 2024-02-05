package com.example.customkafka.modules.broker

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/debug")
class ConfigController(
    val configHandler: ConfigHandler
) {

    @PostMapping("/reload")
    fun reloadConfig() {
        configHandler.start()
    }
}