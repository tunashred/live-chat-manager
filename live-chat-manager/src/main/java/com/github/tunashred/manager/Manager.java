package com.github.tunashred.manager;

import com.github.tunashred.admin.TopicCreator;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Data
@Log4j2
public class Manager {
    static final Path packsDir = Paths.get("packs");
    private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    private static Map<String, List<String>> packs;
    private static KafkaProducer<String, Boolean> producer = null;
    private static KafkaConsumer<String, Boolean> consumer = null;

    public Manager() {
        Properties producerProps = new Properties();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream propsFile = classLoader.getResourceAsStream("producer.properties")) {
            producerProps.load(propsFile);
            producer = new KafkaProducer<>(producerProps);
        } catch (IOException e) {
            log.error("Failed to load producer properties file: ", e);
        }

        Properties consumerProps = new Properties();
        try (InputStream propsFile = classLoader.getResourceAsStream("consumer.properties")) {
            consumerProps.load(propsFile);
            consumer = new KafkaConsumer<>(consumerProps);
        } catch (IOException e) {
            log.error("Failed to load producer properties file: ", e);
        }

        loadPackTopics();
        log.info("Manager ready");
    }

    private static void switchPackTopic(String topic) {
        log.info("Seek to beginning of topic '" + topic + "'");
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(List.of(topicPartition));
        consumer.seekToBeginning(List.of(topicPartition));
    }

    public static List<String> listPacks() {
        System.out.println("Available packs: ");
        List<String> list = new ArrayList<>();
        for (String packName : packs.keySet()) {
            list.add(packName);
            System.out.println(packName);
        }
        return list;
    }

    public static Boolean addWord(String topic, String word) throws IOException {
        List<String> words = packs.get(topic);
        if (words.contains(word)) {
            log.error("Word already inside the pack");
            return false;
        }
        words.add(word);
        producer.send(new ProducerRecord<>(topic, word, true));
        producer.flush();
        log.info("Word added");
        return true;
    }

    public static Boolean addWords(String filePath, String topic) throws IOException {
        Path file = Paths.get(filePath);
        if (!Files.exists(file)) {
            log.error("File not found");
            return false;
        }

        List<String> words = Files.readAllLines(file);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        log.info("Words sent to topic");
        return true;
    }

    public static Boolean deleteWord(String topic, String word) throws IOException {
        List<String> words = packs.get(topic);
        if (words.remove(word)) {
            producer.send(new ProducerRecord<>(topic, word, null));
            producer.flush();
            log.info("Word removed");
            return true;
        }
        log.warn("Word not found");
        return false;
    }

    public static Boolean deletePack(String topic) {
        List<String> words = packs.get(topic);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, null));
        }
        producer.flush();
        packs.remove(topic);
        Boolean success = TopicCreator.deleteTopic(topic);
        if (success) {
            log.info("Pack deleted");
            return true;
        }
        log.error("Unable to delete pack + '" + topic + "'");
        return false;
    }

    public static List<String> searchWord(String word) throws IOException {
        List<String> matches = new ArrayList<>();
        for (var entry : packs.entrySet()) {
            if (entry.getValue().contains(word)) {
                matches.add("Word found in: " + entry.getKey());
            }
        }
        return matches;
    }

    public static List<String> getPack(String topic) throws IOException {
        if (!packs.containsKey(topic)) {
            log.warn("Pack does not exist");
            return Collections.emptyList();
        }
        return packs.get(topic);
    }

    public static Boolean createPackFromFile(String filePath, String packName) throws IOException {
        Path source = Paths.get(filePath);
        if (!Files.exists(source)) {
            log.error("File not found");
            return false;
        }

        String topic = TopicCreator.createPackTopic(packName);
        if (topic == null) {
            log.error("Invalid topic name or topic already exists");
            return false;
        }

        List<String> words = Files.readAllLines(source);

        log.info("Starting to send words from file " + filePath + " to topic " + topic);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        log.info("Successfully sent words to topic.");
        loadPackTopics();
        return true;
    }

    public static void loadPackTopics() {
        packs = new HashMap<>();
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        for (String topic : topicMap.keySet()) {
            if (topic.startsWith("pack-")) {
                List<String> words = readPackTopic(topic);
                packs.put(topic, words);
            }
        }
    }

    private static List<String> readPackTopic(String topic) {
        switchPackTopic(topic);

        List<String> words = new ArrayList<>();
        long lastPollTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - lastPollTime < 1000) {
            log.trace("Polling for pack topic records");
            ConsumerRecords<String, Boolean> records = consumer.poll(500);
            if (!records.isEmpty()) {
                lastPollTime = System.currentTimeMillis();
            }

            for (var record : records) {
                if (record.value() != null) {
                    words.add(record.key());
                }
            }
        }
        return words;
    }

    private static Boolean downloadPack(String topic, String destPath) throws IOException {
        Path path = Paths.get(destPath);
        if (Files.isRegularFile(path) || Files.exists(path)) {
            log.error("File already exists");
            return false;
        }

        List<String> pack = packs.get(topic);
        Files.write(path, pack);
        System.out.println("Pack downloaded.");
        return true;
    }

    public void close() {
        consumer.close();
        producer.close();
    }
}
