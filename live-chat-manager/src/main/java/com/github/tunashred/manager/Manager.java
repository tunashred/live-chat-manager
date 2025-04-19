package com.github.tunashred.manager;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

// TODO: add more checks for edge cases
// TODO 2: check all methods that work with pack names -- they need to use the kafka topic convention checker
@Data
public class Manager {
    static final Path packsDir = Paths.get("packs");
    private static final Logger logger = LogManager.getLogger(Manager.class);
    private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    private static Map<String, List<String>> packs;
    private static KafkaProducer<String, String> producer = null;
    private static KafkaConsumer<String, String> consumer = null;

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

        loadPackTopics();
        logger.info("Manager ready.");
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
                    logger.info("Job's done");
                    return;
                }
                default -> System.out.println("Invalid option.");
            }
        }
    }

    private static void switchPackTopic(String topic) {
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(List.of(topicPartition));
        consumer.seekToBeginning(List.of(topicPartition));
    }

    static void listPacks() {
        System.out.println("Available packs: ");
        for (String packName : packs.keySet()) {
            System.out.println(packName);
        }
    }

    static void editFileMenu() throws IOException {
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
                    2. Concatenate from another file -- not implemented
                    3. Delete a single word
                    4. Delete the pack
                    5. Back
                    """);

            switch (reader.readLine()) {
                case "1" -> addWord(topic);
                case "2" -> {
                    return;
                }
                case "3" -> deleteWord(topic);
                case "4" -> deletePack(topic);
                case "5" -> {
                    return;
                }
                default -> System.out.println("Invalid option.");
            }
        }
    }

    static void addWord(String topic) throws IOException {
        System.out.print("Enter word or expression to add: ");
        String word = reader.readLine();
        if (!word.isEmpty()) {
            List<String> words = packs.get(topic);
            if (words.contains(word)) {
                System.out.println("Word already inside the pack");
                return;
            }
            words.add(word);
            producer.send(new ProducerRecord<>(topic, word, word));
            producer.flush();
            System.out.println("Word added");
        }
    }

    static void deleteWord(String topic) throws IOException {
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

    static void deletePack(String topic) {
        List<String> words = packs.get(topic);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, null));
        }
        producer.flush();
        packs.remove(topic);
        System.out.println("Pack deleted");
    }

    static void searchWord() throws IOException {
        System.out.print("Enter a word to search: ");
        String searchedWord = reader.readLine();

        for (var entry : packs.entrySet()) {
            if (entry.getValue().contains(searchedWord)) {
                System.out.println("Word found in: " + entry.getKey());
            }
        }
    }

    public static boolean isValidKafkaTopicName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        if (name.equals(".") || name.equals("..")) {
            return false;
        }
        if (name.length() > 249) {
            return false;
        }
        return name.matches("[a-zA-Z0-9._-]+");
    }

//    static void concatFile(Path target) throws IOException {
//        System.out.print("Enter path of the file to copy from: ");
//        Path source = Paths.get(reader.readLine());
//        if (!Files.isRegularFile(source) || !Files.exists(source)) {
//            System.out.println("File does not exist");
//            return;
//        }
//
//        List<String> words = Files.readAllLines(source);
//        Files.write(target, words, StandardOpenOption.APPEND);
//        System.out.println("Words added");
//    }

//    // TODO: if the file's name is not good, then it should at least ask for renaming it properly?
//    static void createFromExisting() throws IOException {
//        System.out.print("Enter source file path: ");
//        Path source = Paths.get(reader.readLine());
//        if (!Files.exists(source)) {
//            System.out.println("File not found");
//            return;
//        }
//
//        System.out.print("Enter new file name (without '.txt'): ");
//        String name = reader.readLine();
//        Path dest = packsDir.resolve(name + ".txt");
//
//        Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
//        System.out.println("File created");
//    }

    static void printPack() throws IOException {
        System.out.println("Enter pack name : ");
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

    public void loadPackTopics() {
        packs = new HashMap<>();
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        for (String topic : topicMap.keySet()) {
            if (topic.startsWith("pack-")) {
                List<String> words = readPackTopic(topic);
                packs.put(topic, words);
            }
        }
    }

//    static void renamePack() throws IOException {
//        listPacks();
//        System.out.print("Enter pack name to rename: ");
//        String currentName = reader.readLine();
//        Path current = packsDir.resolve(currentName + ".txt");
//
//        if (!Files.exists(current)) {
//            System.out.println("File does not exist");
//            return;
//        }
//
//        System.out.print("Enter new file name (without '.txt'): ");
//        String newName = reader.readLine();
//        if (!isValidKafkaTopicName(newName)) {
//            System.out.println("The file name does not meet Kafka topics naming convention");
//            return;
//        }
//        Path newFile = packsDir.resolve(newName + ".txt");
//        if (Files.exists(newFile)) {
//            System.out.println("File already exists");
//            return;
//        }
//
//        Files.move(current, newFile, StandardCopyOption.REPLACE_EXISTING);
//        System.out.println("File renamed");
//    }

    private List<String> readPackTopic(String topic) {
        switchPackTopic(topic);

        List<String> words = new ArrayList<>();
        long lastPollTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - lastPollTime < 1000) {
            ConsumerRecords<String, String> records = consumer.poll(500);
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

    public static void createPackFromFile() throws IOException {
        System.out.print("Enter file path: ");
        String filePath = reader.readLine();

        Path source = Paths.get(filePath);
        if (!Files.exists(source)) {
            System.out.println("File not found");
            return;
        }

        System.out.print("Enter new pack name: ");
        String topic;
        while (true) {
            topic = reader.readLine();
            if (isValidKafkaTopicName(topic)) {
                break;
            }
            System.out.println("Invalid pack name. Please try again");
            System.out.println("Format: \"pack-<name>\"");
        }

        List<String> words = Files.readAllLines(source);

        logger.info("Starting to send words from file " + filePath + " to topic " + topic);
        for (String word : words) {
            producer.send(new ProducerRecord<>(topic, word, word));
        }
        producer.flush();
        logger.info("Successfully sent words to topic.");
    }

    public void close() {
        consumer.close();
        producer.close();
    }
}
