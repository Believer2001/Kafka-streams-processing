package enset.ma;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import  java.util.Properties;

public class App1 {
    public static void main(String[] args) {
// Configurer l'application Kafka Streams
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
// Construire le flux
        StreamsBuilder builder = new StreamsBuilder();

        // 2. lecture des messages du topic text-input
        KStream<String, String> sourceStream = builder.stream("text-input");
// Transformation :
        //3. suppression des espaces avant/apres
        KStream<String, String> supEspace = sourceStream.mapValues(value -> value.trim());
        // 2. Remplacer les espaces multiples par un seul espace
        KStream<String, String> singleSpaceStream = supEspace.mapValues(value ->
                value.replaceAll("\\s+", " ")
        );

        // 3. Convertir la chaîne en majuscules
        KStream<String, String> upperCaseStream = singleSpaceStream.mapValues(value ->
                value.toUpperCase()
        );

        //4. filtre


        // --- 4. FILTRAGE ET RÈGLES DE VALIDATION

        final int MAX_LENGTH = 100;
        // La liste des mots interdits est en majuscules pour correspondre au flux upperCaseStream
        final String[] FORBIDDEN_WORDS = {"HACK", "SPAM", "XXX", "VIOLENCE", "FRAUD"};

        // Définition du prédicat de validation (retourne TRUE si le message est valide)
        Predicate<String, String> isMessageValid = (key, value) -> {
            // Règle 1: Rejeter les messages vides.
            if (value.isEmpty()) {
                System.out.println("[REJETÉ] (Vide): Clé=" + key);
                return false;
            }

            // Règle 2: Rejeter les messages contenant des mots interdits
            for (String word : FORBIDDEN_WORDS) {
                if (value.contains(word)) {
                    System.out.println("[REJETÉ] (Mot Interdit: " + word + "): " + value);
                    return false;
                }
            }

            // Règle 3: Rejeter les messages dépassant la longueur maximale
            if (value.length() > MAX_LENGTH) {
                System.out.println("[REJETÉ] (Trop Long: " + value.length() + "): " + value.substring(0, Math.min(value.length(), 15)) + "...");
                return false;
            }

            // Le message est valide
            System.out.println("[ACCEPTÉ]: " + value);
            return true;
        };

        // --- 6. Routage : Utilisation de branch pour diviser le flux en messages valides et invalides

        KStream<String, String>[] branches = upperCaseStream.branch(
                isMessageValid, // Premier Prédicat (Index 0): Messages valides
                (key, value) -> true // Deuxième Prédicat (Index 1): Tout le reste (messages invalides)
        );

        // Les messages valides (branche 0) sont envoyés au topic text-clean
        branches[0].to("text-clear");
        System.out.println("   --> Messages valides routés vers 'text-clean'");

        // Les messages invalides (branche 1) sont envoyés au topic text-dead-letter
        branches[1].to("text-dead-letter");
        System.out.println("   --> Messages invalides routés vers 'text-dead-letter'");


        // --- Démarrer l'application Kafka Streams
        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
            System.out.println("\n--- Application Kafka Streams DÉMARRÉE. En attente de messages sur 'text-input'...");
            System.out.println("    Appuyez sur Ctrl+C pour arrêter proprement.");
            Thread.currentThread().join();
            // Ajouter un hook pour arrêter proprement l'application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        } catch (Exception e) {
            System.err.println("Erreur lors du démarrage de Kafka Streams: " + e.getMessage());
        }



    }
}
