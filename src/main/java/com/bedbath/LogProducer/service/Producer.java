package com.bedbath.LogProducer.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.bedbath.LogProducer.Constant;

@Service
public class Producer {

	final Logger logger = Logger.getLogger(Producer.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;


	public void sendMessage(String flag){
		File file = null;
		String topicName = null;
		if(flag.equalsIgnoreCase("logs")){
			 file = new File("C:\\Users\\BA61744\\Desktop\\Report\\loadlog_1.txt");
			 topicName=Constant.LOGS_TOPIC;
		}
		else if(flag.equalsIgnoreCase("oh"))
		{
			 file = new File("C:\\Users\\BA61744\\Desktop\\Report\\orderHeader.csv");
			 topicName=Constant.OH_TOPIC;
		}

		logger.info("after reading file "+new Date());

		try (BufferedReader br = new BufferedReader(new FileReader(file)))
		{		
			String st; 
			while ((st = br.readLine()) != null) 
			{
				System.out.println("1");
				try{
					this.kafkaTemplate.send(topicName, st);
					System.out.println("2");

				}
				catch (Exception e) {
					System.out.println("asd");			
				}

			}
		} catch (FileNotFoundException e) {
			logger.info("getting exception "+ e.getMessage()+" "+new Date());
			e.printStackTrace();
		} catch (IOException e) {
			logger.info("getting exception "+ e.getMessage()+" "+new Date());
			e.printStackTrace();
		} 

	}
}
