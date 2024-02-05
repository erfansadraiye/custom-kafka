package com.example.customkafka.modules.broker

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/config")
class ConfigController(
    val configHandler: ConfigHandler
) {

    @PostMapping("/reload")
    fun reloadConfig(): ResponseEntity<*> {
        configHandler.start()
        return ResponseEntity.ok("Config reloaded!")
    }


}