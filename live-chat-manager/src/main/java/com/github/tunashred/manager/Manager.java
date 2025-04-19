package com.github.tunashred.manager;

import lombok.Data;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// TODO: add more checks for edge cases
@Data
public class Manager {
    private static final Logger logger = LogManager.getLogger(Manager.class);
    private static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    static final Path packsDir = Paths.get("packs");
    private static KafkaProducer<String, String> producer = null;
    private static KafkaConsumer<String, String> consumer = null;
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
        Files.createDirectories(packsDir);
        Manager manager = new Manager();

        while (true) {
            System.out.println("""
                \nMenu:
                1. Show pack names
                2. Select pack to edit
                3. Create new pack from another
                4. Search for a word
                5. Rename a pack
                6. Select a pack to print contents
                7. Exit
                """);

            switch (reader.readLine()) {
                case "1" -> listFiles();
                case "2" -> editFileMenu();
                case "3" -> createFromExisting();
                case "4" -> searchWord();
                case "5" -> renameFile();
                case "6" -> printFileContents();
                case "7" -> {
                    manager.close();
                    logger.info("Job's done");
                    return;
                }
                default -> System.out.println("Invalid option.");
            }
        }
    }

    static void listFiles() throws IOException {
        System.out.println("Available packs:");
        Files.list(packsDir)
                .filter(Files::isRegularFile)
                .map(path -> path.getFileName().toString().replaceAll("\\.txt$", ""))
                .forEach(System.out::println);
    }

    static void editFileMenu() throws IOException {
        listFiles();
        System.out.println("Enter file name to edit (without '.txt'): ");
        String fileName = reader.readLine();
        Path file = packsDir.resolve(fileName + ".txt");

        if (!Files.isRegularFile(file) || !Files.exists(file)) {
            System.out.println("File does not exist");
            return;
        }

        while (true) {
            System.out.println("""
                \nEditing Menu:
                1. Add single word
                2. Concatenate from another file
                3. Delete a single word
                4. Delete the file
                5. Back
                """);

            switch (reader.readLine()) {
                case "1" -> addWord(file);
                case "2" -> concatFile(file);
                case "3" -> deleteWord(file);
                case "4" -> deleteFile(file);
                case "5" -> { return; }
                default -> System.out.println("Invalid option.");
            }
        }
    }

    static void addWord(Path file) throws IOException {
        System.out.print("Enter word or expression to add: ");
        String word = reader.readLine();
        if (!word.isEmpty()) {
            List<String> words = new ArrayList<>(Files.readAllLines(file));
            if (words.contains(word)) {
                System.out.println("Word already inside the pack");
                return;
            }
            Files.write(file, List.of(word), StandardOpenOption.APPEND);
            System.out.println("Word added");
        }
    }

    static void concatFile(Path target) throws IOException {
        System.out.print("Enter path of the file to copy from: ");
        Path source = Paths.get(reader.readLine());
        if (!Files.isRegularFile(source) || !Files.exists(source)) {
            System.out.println("File does not exist");
            return;
        }

        List<String> words = Files.readAllLines(source);
        Files.write(target, words, StandardOpenOption.APPEND);
        System.out.println("Words added");
    }

    static void deleteWord(Path file) throws IOException {
        List<String> words = new ArrayList<>(Files.readAllLines(file));
        System.out.print("Enter word to delete: ");
        String word = reader.readLine();
        if (words.remove(word)) {
            Files.write(file, words);
            System.out.println("Word removed");
        } else {
            System.out.println("Word not found");
        }
    }

    static void deleteFile(Path file) throws IOException {
        Files.delete(file);
        System.out.println("File deleted");
    }

    // TODO: if the file's name is not good, then it should at least ask for renaming it properly?
    static void createFromExisting() throws IOException {
        System.out.print("Enter source file path: ");
        Path source = Paths.get(reader.readLine());
        if (!Files.exists(source)) {
            System.out.println("File not found");
            return;
        }

        System.out.print("Enter new file name (without '.txt'): ");
        String name = reader.readLine();
        Path dest = packsDir.resolve(name + ".txt");

        Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
        System.out.println("File created");
    }

    static void searchWord() throws IOException {
        System.out.print("Enter a word to search: ");
        String word = reader.readLine();

        Files.list(packsDir)
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        List<String> lines = Files.readAllLines(file);
                        if (lines.contains(word)) {
                            System.out.println("Word found in: " + file.getFileName());
                        }
                    } catch (IOException e) {
                        System.out.println("Could not read file " + file);
                    }
                });
    }

    public static boolean isValidKafkaTopicName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        if (name.equals(".") || name.equals("..")) {
            return false;
        }
        return name.matches("[a-zA-Z0-9._-]+");
    }


    static void renameFile() throws IOException {
        listFiles();
        System.out.print("Enter file name to rename (without '.txt'): ");
        String currentName = reader.readLine();
        Path current = packsDir.resolve(currentName + ".txt");

        if (!Files.exists(current)) {
            System.out.println("File does not exist");
            return;
        }

        System.out.print("Enter new file name (without '.txt'): ");
        String newName = reader.readLine();
        if (!isValidKafkaTopicName(newName)) {
            System.out.println("The file name does not meet Kafka topics naming convention");
            return;
        }
        Path newFile = packsDir.resolve(newName + ".txt");
        if (Files.exists(newFile)) {
            System.out.println("File already exists");
            return;
        }

        Files.move(current, newFile, StandardCopyOption.REPLACE_EXISTING);
        System.out.println("File renamed");
    }

    static void printFileContents() throws IOException {
        listFiles();
        System.out.println("Enter file name to edit (without '.txt'): ");
        String fileName = reader.readLine();
        Path file = packsDir.resolve(fileName + ".txt");

        if (!Files.isRegularFile(file) || !Files.exists(file)) {
            System.out.println("File does not exist");
            return;
        }

        System.out.println(file.getFileName() + " contents: ");
        Files.readAllLines(file).forEach(System.out::println);
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
