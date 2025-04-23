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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

// TODO: add more checks for edge cases
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
        try (InputStream propsFile = new FileInputStream("src/main/resources/producer.properties")) {
            producerProps.load(propsFile);
            producer = new KafkaProducer<>(producerProps);
        } catch (IOException e) {
            log.error("Failed to load producer properties file: ", e);
        }

        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream("src/main/resources/consumer.properties")) {
            consumerProps.load(propsFile);
            consumer = new KafkaConsumer<>(consumerProps);
        } catch (IOException e) {
            log.error("Failed to load producer properties file: ", e);
        }

        loadPackTopics();
        log.info("Manager ready.");
    }

    public static void main(String[] args) throws IOException {
        Files.createDirectories(packsDir);
        Manager manager = new Manager();

        while (true) {
            System.out.println("""
                    \nMenu:
                    1. Show pack names
                    2. Select pack to edit
                    3. Create new pack by file
                    4. Search for a word
                    5. Rename a pack -- not implemented
                    6. Select a pack to print contents
                    7. Exit
                    """);

            switch (reader.readLine()) {
                case "1" -> listPacks();
                case "2" -> editFileMenu();
                case "3" -> createPackFromFile();
                case "4" -> searchWord();
                case "5" -> {
                    return;
                }
                case "6" -> printPack();
                case "7" -> {
                    manager.close();
                    log.info("Job's done");
                    return;
                }
                default -> System.out.println("Invalid option.");
            }
        }
    }

    private static void switchPackTopic(String topic) {
        log.info("Seek to beginning of topic '" + topic + "'");
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(List.of(topicPartition));
        consumer.seekToBeginning(List.of(topicPartition));
    }

    public static void listPacks() {
        System.out.println("Available packs: ");
        for (String packName : packs.keySet()) {
            System.out.println(packName);
        }
    }

    public static void editFileMenu() throws IOException {
        listPacks();
        System.out.println("Enter pack name to edit: ");
        String topic;
        while (true) {
            topic = reader.readLine();

            if (packs.containsKey(topic)) {
                break;
            }
            System.out.println("Topic pack name is incorrect, try again");
        }
        switchPackTopic(topic);

        while (true) {
            System.out.println("""
                    \nEditing Menu:
                    1. Add single word
                    2. Add words from file
                    3. Delete a single word
                    4. Delete the pack
                    5. Download pack
                    6. Back
                    """);

            switch (reader.readLine()) {
                case "1" -> addWord(topic);
                case "2" -> addWords(topic);
                case "3" -> deleteWord(topic);
                case "4" -> {
                    deletePack(topic);
                    return;
                }
                case "5" -> downloadPack(topic);
                case "6" -> {
                    return;
                }
                default -> System.out.println("Invalid option.");
            }
        }
    }

    public static void addWord(String topic) throws IOException {
        System.out.print("Enter word or expression to add: ");
        String word = reader.readLine();
        if (!word.isEmpty()) {
            List<String> words = packs.get(topic);
            if (words.contains(word)) {
                System.out.println("Word already inside the pack");
                return;
            }
            words.add(word);
            producer.send(new ProducerRecord<>(topic, word, true));
            producer.flush();
            System.out.println("Word added");
        }
    }

    public static void addWords(String topic) throws IOException {
        System.out.println("Enter file path: ");
        String filePath = reader.readLine();

        Path file = Paths.get(filePath);
        if (!Files.exists(file)) {
            System.out.println("File not found");
            return;
        }

        List<String> words = Files.readAllLines(file);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        System.out.println("Words sent to topic");
    }

    public static void deleteWord(String topic) throws IOException {
        List<String> words = packs.get(topic);
        System.out.print("Enter word to delete: ");
        String word = reader.readLine();
        if (words.remove(word)) {
            producer.send(new ProducerRecord<>(topic, word, null));
            producer.flush();
            System.out.println("Word removed");
        } else {
            System.out.println("Word not found");
        }
    }

    public static void deletePack(String topic) {
        List<String> words = packs.get(topic);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, null));
        }
        producer.flush();
        packs.remove(topic);
        TopicCreator.deleteTopic(topic);
        System.out.println("Pack deleted");
    }

    public static void searchWord() throws IOException {
        System.out.print("Enter a word to search: ");
        String searchedWord = reader.readLine();

        for (var entry : packs.entrySet()) {
            if (entry.getValue().contains(searchedWord)) {
                System.out.println("Word found in: " + entry.getKey());
            }
        }
    }

    public static void printPack() throws IOException {
        System.out.println("Enter pack name: ");
        String topic;
        while (true) {
            topic = reader.readLine();

            if (packs.containsKey(topic)) {
                break;
            }
            System.out.println("Topic pack name is incorrect, try again");
        }

        System.out.println(topic + " contents: ");
        packs.get(topic).forEach(System.out::println);
    }

    public static void createPackFromFile() throws IOException {
        System.out.print("Enter file path: ");
        String filePath = reader.readLine();

        Path source = Paths.get(filePath);
        if (!Files.exists(source)) {
            System.out.println("File not found");
            return;
        }

        System.out.print("Enter new pack name: ");
        String topic = reader.readLine();
        topic = TopicCreator.createPackTopic(topic);
        if (topic == null) {
            System.out.println("Invalid topic name or topic already exists");
            return;
        }

        List<String> words = Files.readAllLines(source);

        log.info("Starting to send words from file " + filePath + " to topic " + topic);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, true));
        }
        producer.flush();
        log.info("Successfully sent words to topic.");
        loadPackTopics();
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

    private static void downloadPack(String topic) throws IOException {
        System.out.print("Add path for file: ");
        String destPath = reader.readLine();
        Path path = Paths.get(destPath);
        if (Files.isRegularFile(path) || Files.exists(path)) {
            System.out.println("File already exists");
            return;
        }

        List<String> pack = packs.get(topic);
        Files.write(path, pack);
        System.out.println("Pack downloaded.");
    }

    public void close() {
        consumer.close();
        producer.close();
    }
}
