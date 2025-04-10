package com.github.tunashred.manager;

import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// TODO: add more checks for edge cases
public class Manager {
    private static final Logger logger = LogManager.getLogger(Manager.class);
    private static KafkaProducer<String, String> producer = null;
    private static KafkaConsumer<String, String> consumer = null;
    @Setter
    private String topic;

    public Manager() {
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
        } catch (IOException e) {
            logger.error("Failed to load producer properties file: " + e);
            throw new RuntimeException();
        }
        // TODO: should I wrap this into a try statement too?
        producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
        } catch (IOException e) {
            logger.error("Failed to load producer properties file: " + e);
            throw new RuntimeException();
        }
        consumer = new KafkaConsumer<>(consumerProps);
    }

    public static void main(String[] args) throws IOException {
        Manager manager = new Manager();

//        manager.manageTopic();

        manager.setTopic("banned-words");
        manager.sendWordsFromFile("packs/universal_banned_words.txt");

//        manager.saveWordsToFile("packs/banned-words-backup.txt");

        manager.close();

        logger.info("Job's done");
    }

    public void sendWordsFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            logger.info("Starting to send words from file " + filePath + " to topic " + topic);
            String line;
            while ((line = reader.readLine()) != null) {
                String word = line.trim();
                if (!word.isEmpty()) {
                    producer.send(new ProducerRecord<>(topic, word, word));
                }
            }
            logger.info("Successfully sent words to topic.");
        } catch (IOException e) {
            logger.error("Failed inside sendWordsFromFile: ", e);
        }
    }

    // TODO: for future, this code must be more robust and cleaner
    public void saveWordsToFile(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), "utf-8"))) {
            List<String> wordsList = new ArrayList<>();
            consumer.subscribe(Collections.singletonList("banned-words"));

            logger.info("Successfully subscribed to topic" + topic + " and waiting to receive records");
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(6000));
                if (consumerRecords.isEmpty()) {
                    logger.info("Done consuming records.");
                    break;
                }
                for (var record : consumerRecords) {
                    wordsList.add(record.key());
                    logger.info("Consumed record key: " + record.key());
                }
            }
            consumer.close();

            logger.info("Started writing words to file.");
            for (String word : wordsList) {
                writer.write(word);
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("Failed inside saveWordsToFile while trying to save words to file: ", e);
            throw new RuntimeException(e);
        }
    }

    public void manageTopic() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Add compacted topic name:");
        String input = reader.readLine().trim();

        // since anyone can create topics if they do not exist, I need to make sure this won't happen accidentally
        // this could be a barbaric way to add valid topics for the operations done by this class
        // though, it might not matter so much since this would be a private tool
        List<String> topics = new ArrayList<>();
        topics.add("banned-words");

        if (!topics.contains(input)) {
            System.out.println("Invalid topic name!");
            return;
        }
        setTopic(input);

        int option = 0;
        while (option < 1 || option > 2) {
            System.out.println("1. Add a word\n2. Remove a word\n");
            input = reader.readLine().trim();
            option = Integer.parseInt(input);
            if (option < 1 || option > 2) {
                System.out.println("Invalid option.\n");
            }
        }

        // TODO: I could maybe have an option to give a list?
        if (option == 1) { // add word logic
            System.out.println("Word to add to " + topic + " topic: ");
            String word = reader.readLine().trim();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, word, word);
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    logger.error("Failed while trying to add word " + word + " to topic " + topic, e);
                } else {
                    logger.info("Word added");
                }
            });
        } else if (option == 2) { // remove word logic
            System.out.println("Word to remove to " + topic + " topic: ");
            input = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, input, null);
            producer.send(record);
        }
        producer.flush();
    }

    public void close() {
        consumer.close();
        producer.close();
    }
}
