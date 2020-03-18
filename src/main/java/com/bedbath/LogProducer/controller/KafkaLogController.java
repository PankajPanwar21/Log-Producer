package com.bedbath.LogProducer.controller;

import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.bedbath.LogProducer.service.Producer;


@RestController
@RequestMapping(value = "/kafka")
public class KafkaLogController {

	final Logger logger = Logger.getLogger(KafkaLogController.class);

	private final Producer producer;

	@Autowired
	KafkaLogController(Producer producer) {
		this.producer = producer;
	}

	@GetMapping(value = "/sendLogs")
	public String sendLogsToKafkaTopic() {

		logger.info("getting request at sendLogsToKafkaTopic method at "+new Date());

		this.producer.sendMessage("logs");
		logger.info("Message successfully sent to Kafka broker at "+new Date());
		return "Message successfully sent to Kafka broker at "+new Date();

	}
	
	@GetMapping(value = "/sendOhData")
	public String sendOhDataToKafkaTopic() {

		logger.info("getting request at sendOhDataToKafkaTopic method at "+new Date());

		this.producer.sendMessage("oh");
		logger.info("Message successfully sent to Kafka broker at "+new Date());
		return "Message successfully sent to Kafka broker at "+new Date();

	}

}
