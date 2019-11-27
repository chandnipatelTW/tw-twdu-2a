package com.free2wheelers;

import com.free2wheelers.services.ApiProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;

@Component
public class ProducerScheduler {
    private static Logger logger = LoggerFactory.getLogger(ApiProducer.class);
    @Autowired
    private ApiProducer apiProducer;

    @Value("${producer.url}")
    private String url;

    @Scheduled(cron="${producer.cron}")
    public void scheduledProducer() {

        RestTemplate template = new RestTemplate();
        logger.info("Making api call", url);
        HttpEntity<String> response = template.exchange("http://mock-server:5001/networks/ford-gobike", HttpMethod.GET, HttpEntity.EMPTY, String.class);
        logger.info("Made api call", url);
        logger.info("navya", response.toString());
        logger.info("navya");
        apiProducer.sendMessage(response);
    }
}
