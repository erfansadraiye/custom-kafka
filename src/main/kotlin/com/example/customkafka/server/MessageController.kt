package com.example.customkafka.server

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController


@RestController
@RequestMapping("/message")
class MessageController(
    val messageService: MessageService
) {


    @PostMapping("/send")
    fun sendMessage(
        @RequestBody request: MessageRequest
    ): ResponseEntity<*> {
        messageService.sendMessage(request.key, request.message)
        return ResponseEntity.ok("Message sent successfully!")
    }


}

data class MessageRequest(
    val key: String,
    val message: String
)
