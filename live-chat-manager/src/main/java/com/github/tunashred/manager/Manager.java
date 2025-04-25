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
        log.trace("Loading producer properties");
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

        log.trace("Loading consumer properties");
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

    public static List<String> listPacks() {
        return new ArrayList<>(packs.keySet());
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

    public static List<String> getPack(String topic) {
        String topicName = packanizeTopicName(topic);
        if (!packs.containsKey(topicName)) {
            log.warn("Pack does not exist");
            return Collections.emptyList();
        }
        return packs.get(topicName);
    }

    public static boolean createPackFromFile(InputStream inputStream, String pack) throws IOException {
        String topic = packanizeTopicName(pack);
        if (topicExists(topic)) {
            log.error("Topic named '{}' already exists", pack);
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
        log.trace("List of words added to new pack topic '{}': {}", topic, words);

        log.trace("Creating topic using Admin API");
        topic = TopicCreator.createPackTopic(topic);
        if (topic == null) {
            log.error("Invalid topic name or topic already exists");
            return false;
        }

        log.trace("Starting to send words to topic {}", pack);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        log.info("Successfully sent words to topic.");
        loadPackTopics();
        return true;
    }

    public static boolean addWord(String topic, String word) {
        String topicName = packanizeTopicName(topic);
        List<String> words = packs.get(topicName);
        if (words.contains(word)) {
            log.error("Word already inside the pack");
            return false;
        }
        words.add(word);
        producer.send(new ProducerRecord<>(topicName, word, true));
        producer.flush();
        log.info("Word added");
        return true;
    }

    public static boolean addWords(InputStream inputStream, String topic) throws IOException {
        String topicName = packanizeTopicName(topic);
        log.trace("Appending words to existing pack topic '{}'", topicName);

        List<String> words;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            words = reader.lines().toList();
        }
        if (words.isEmpty()) {
            log.error("File contents are empty");
            return false;
        }

        for (String word : words) {
            if (packs.get(topicName).contains(word)) {
                log.warn("Pack '{}' already contains word ''", topicName);
                continue;
            }
            log.trace("Sending word '{}' to topic '{}'", word, topicName);
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        log.info("Words sent to topic '{}'", topicName);
        return true;
    }

    public static boolean deleteWord(String topic, String word) {
        String topicName = packanizeTopicName(topic);
        List<String> words = packs.get(topicName);
        if (words.remove(word)) {
            producer.send(new ProducerRecord<>(topicName, word, null));
            producer.flush();
            log.trace("Word '{}' removed from pack topic '{}'", word, topicName);
            return true;
        }
        log.error("Word not found");
        return false;
    }

    public static boolean deletePack(String topic) throws JsonProcessingException {
        String topicName = packanizeTopicName(topic);
        if (!topicExists(topicName)) {
            log.error("Pack named '{}' does not exist", topic);
            return false;
        }
        log.info("Tombstoning all pack '{}' records", topic);
        List<String> words = packs.get(topicName);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topicName, word, null));
        }
        producer.flush();
        packs.remove(topicName);
        boolean success = TopicCreator.deletePackTopic(topicName);
        if (!success) {
            log.error("Unable to delete topic pack + '{}'", topicName);
            return false;
        }

        Map<String, List<String>> streamerPreferences = Streamer.getPreferencesMap();
        if (streamerPreferences.isEmpty()) {
            log.warn("Streamer has no preferences");
            return false;
        }
        for (var entry : streamerPreferences.entrySet()) {
            if (entry.getValue().contains(topicName)) {
                Streamer.removePreference(entry.getKey(), topicName);
                log.trace("Removed pack '{}' from streamer '{}' preferences", topic, entry.getKey());
            }
        }
        log.info("Pack {} deleted", topic);
        return true;
    }

    private static void switchPackTopic(String topic) {
        log.info("Seek to beginning of topic '{}'", topic);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(List.of(topicPartition));
        consumer.seekToBeginning(List.of(topicPartition));
    }

    private static void loadPackTopics() {
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

    private static List<String> readPackTopic(String topic) {
        String topicName = packanizeTopicName(topic);
        switchPackTopic(topicName);

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
                    log.trace("Consumed word '{}' from topic '{}'", record.key(), topicName);
                }
            }
        }
        return words;
    }

    // still debating if this should exist or not
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

    private static String packanizeTopicName(String topic) {
        if (!topic.startsWith("pack-")) {
            return "pack-" + topic;
        }
        return topic;
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
