package com.example.customkafka.modules.broker

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
        return try {
            messageService.sendMessage(request.key, request.message)
            ResponseEntity.ok("Message sent successfully!")
        } catch (e: Exception) {
            ResponseEntity.badRequest().body("Error sending message")
        }
    }

    @PostMapping("/replica")
    fun getReplicaMessages(
        @RequestBody message: Message
    ): ResponseEntity<*> {
        return try {
            messageService.getReplicaMessages(message)
            ResponseEntity.ok("Message sent to replica successfully!")
        } catch (e: Exception) {
            ResponseEntity.badRequest().body("Error sending message to replica")
        }
    }

}

data class MessageRequest(
    val key: String,
    val message: String
)
