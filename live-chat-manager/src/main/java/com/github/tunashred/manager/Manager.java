package com.github.tunashred.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.admin.TopicCreator;
import com.github.tunashred.streamer.Streamer;
import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
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
import java.time.Duration;
import java.util.*;

@Data
@Log4j2
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Manager {
    static String PREFERENCES_TOPIC = "streamer-preferences";
    static Map<String, List<String>> packs;
    static KafkaProducer<String, Boolean> producer = null;
    static KafkaConsumer<String, Boolean> consumer = null;
    Streamer streamer;

    public Manager(Streamer streamer) {
        Properties producerProps = new Properties();
        try (InputStream propsFile = Manager.class.getClassLoader().getResourceAsStream("manager-producer.properties")) {
            if (propsFile == null) {
                log.error("Cannot find manager-producer.properties in classpath");
            }
            producerProps.load(propsFile);
            producer = new KafkaProducer<>(producerProps);
        } catch (IOException e) {
            log.error("Failed to load producer properties file: ", e);
        }

        Properties consumerProps = new Properties();
        try (InputStream propsFile = Manager.class.getClassLoader().getResourceAsStream("manager-consumer.properties")) {
            if (propsFile == null) {
                log.error("Cannot find manager-consumer.properties in classpath");
            }
            consumerProps.load(propsFile);
            consumer = new KafkaConsumer<>(consumerProps);
        } catch (IOException e) {
            log.error("Failed to load producer properties file: ", e);
        }

        loadPackTopics();

        this.streamer = streamer;
        log.info("Manager ready");
    }

    private static void switchPackTopic(String topic) {
        log.info("Seek to beginning of topic '{}'", topic);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(List.of(topicPartition));
        consumer.seekToBeginning(List.of(topicPartition));
    }

    public static List<String> listPacks() {
        return new ArrayList<>(packs.keySet());
    }

    public static boolean addWord(String topicName, String word) {
        String topic = packanizeTopicName(topicName);
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

    public static boolean addWords(InputStream inputStream, String topicName) throws IOException {
        String topic = packanizeTopicName(topicName);
        log.info("Appending words to existing pack topic '{}'", topic);

        List<String> words;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            words = reader.lines().toList();
        }
        if (words.isEmpty()) {
            log.error("File contents are empty");
            return false;
        }

        for (String word : words) {
            if (packs.get(topic).contains(word)) {
                log.warn("Pack '{}' already contains word ''", topicName);
                continue;
            }
            log.trace("Sending word '{}' to topic '{}'", word, topic);
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        log.info("Words sent to topic");
        return true;
    }

    public static boolean deleteWord(String topicName, String word) {
        String topic = packanizeTopicName(topicName);
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

    // TODO: make sure the pack is removed from the preferences topic
    public static boolean deletePack(String topicName) throws JsonProcessingException {
        String topic = packanizeTopicName(topicName);
        if (!topicExists(topic)) {
            log.error("Pack named '{}' does not exist", topicName);
            return false;
        }
        log.info("Tombstoning all pack '{}' records", topicName);
        List<String> words = packs.get(topic);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, null));
        }
        producer.flush();
        packs.remove(topic);
        boolean success = TopicCreator.deletePackTopic(topic);
        if (!success) {
            log.error("Unable to delete pack + '{}'", topic);
            return false;
        }

        Map<String, List<String>> streamerPreferences = Streamer.getPreferencesMap();
        if (streamerPreferences.isEmpty()) {
            log.warn("Streamer has no preferences");
            return false;
        }
        for (var entry : streamerPreferences.entrySet()) {
            if (entry.getValue().contains(topic)) {
                Streamer.removePreference(entry.getKey(), topic);
                log.trace("Removed pack '{}' from streamer '{}' preferences", topicName, entry.getKey());
            }
        }
        log.info("Pack {} deleted", topicName);
        return true;
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

    public static List<String> getPack(String topicName) {
        String topic = packanizeTopicName(topicName);
        if (!packs.containsKey(topic)) {
            log.warn("Pack does not exist");
            return Collections.emptyList();
        }
        return packs.get(topic);
    }

    public static boolean createPackFromFile(InputStream inputStream, String packName) throws IOException {
        String topic = packanizeTopicName(packName);
        if (topicExists(topic)) {
            log.error("Topic named '{}' already exists", packName);
            return false;
        }
        log.info("Creating new pack topic from file contents provided");
        List<String> words;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            words = reader.lines().toList();
        }
        if (words.isEmpty()) {
            log.error("File contents are empty");
            return false;
        }

        log.info("Creating topic with Admin API");
        topic = TopicCreator.createPackTopic(topic);
        if (topic == null) {
            log.error("Invalid topic name or topic already exists");
            return false;
        }

        log.info("Starting to send words to topic {}", packName);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        log.info("Successfully sent words to topic.");
        loadPackTopics();
        return true;
    }

    public static void loadPackTopics() {
        log.info("Loading pack topics");
        packs = new HashMap<>();
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        for (String topic : topicMap.keySet()) {
            if (topic.startsWith("pack-")) {
                List<String> words = readPackTopic(topic);
                packs.put(topic, words);
            }
        }
    }

    private static List<String> readPackTopic(String topicName) {
        String topic = packanizeTopicName(topicName);
        switchPackTopic(topic);

        List<String> words = new ArrayList<>();
        long lastPollTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - lastPollTime < 1000) {
            log.trace("Polling for pack topic records");
            ConsumerRecords<String, Boolean> records = consumer.poll(Duration.ofMillis(500));
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

    private static boolean downloadPack(String topicName, String destPath) throws IOException {
        String topic = packanizeTopicName(topicName);
        if (!topicExists(topic)) {
            log.error("Topic named '{}' does not exist", topicName);
            return false;
        }
        Path path = Paths.get(destPath);
        if (Files.isRegularFile(path) || Files.exists(path)) {
            log.error("File already exists");
            return false;
        }

        List<String> pack = packs.get(topic);
        Files.write(path, pack);
        log.info("Pack downloaded");
        return true;
    }

    private static String packanizeTopicName(String topicName) {
        if (!topicName.startsWith("pack-")) {
            return "pack-" + topicName;
        }
        return topicName;
    }

    private static boolean topicExists(String topic) {
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        return topicMap.containsKey(topic);
    }

    public void close() {
        consumer.close();
        producer.close();
        streamer.close();
    }
}
