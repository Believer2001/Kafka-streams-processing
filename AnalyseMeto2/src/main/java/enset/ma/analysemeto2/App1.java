package enset.ma.analysemeto2;

import java.util.Properties;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class App1 {


    public static void main(String[] ags) {

//Configurer l 'application Kafka Streams
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");







        // Construire le flux

        StreamsBuilder builder = new StreamsBuilder();

         //1 Lecture des messades du topic weather-data  en utilisant le flux KStream

        KStream<String, String> sourceStream = builder.stream("weather-data");


        //2 Filtrer les données de température élevée (superieure à 30°C)

        KStream<String, String> highTempStream = sourceStream.filter((key, value) -> {
            String[] parts = value.split(",");
            double temperature = Double.parseDouble(parts[1]);
            System.out.println("Temperature: " + temperature);
            return temperature > 30.0;
        });

        // 3 Convertir les temperatures en Fahrenheit

        KStream<String, String> fahrenheitStream = highTempStream.mapValues(value -> {
            String[] parts = value.split(",");
            String station = parts[0];
            double celsius = Double.parseDouble(parts[1]);
            double humidity = Double.parseDouble(parts[2]);
            double fahrenheit = (celsius * 9/5) + 32;
            return station + "," + fahrenheit + "," + humidity;
        });

        // 4 Grouper les données par station et calculer la température moyenne et le taux d'humidité moyen
        KGroupedStream<String, String> groupedStream = fahrenheitStream.groupBy((key, value) -> {
            String[] parts = value.split(",");
            return parts[0]; // Grouper par station
        });

        KTable<String, String> aggregatedTable = groupedStream.aggregate(
                () -> "0,0,0", // Initializer: "sumTemp,sumHumidity,count"
                (station, newValue, aggregate) -> {
                    String[] parts = newValue.split(",");
                    double temp = Double.parseDouble(parts[1]);
                    double humidity = Double.parseDouble(parts[2]);

                    String[] aggParts = aggregate.split(",");
                    double sumTemp = Double.parseDouble(aggParts[0]) + temp;
                    double sumHumidity = Double.parseDouble(aggParts[1]) + humidity;
                    int count = Integer.parseInt(aggParts[2]) + 1;

                    return sumTemp + "," + sumHumidity + "," + count;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        KTable<String, String> averageTable = aggregatedTable.mapValues(value -> {
            String[] parts = value.split(",");
            double sumTemp = Double.parseDouble(parts[0]);
            double sumHumidity = Double.parseDouble(parts[1]);
            int count = Integer.parseInt(parts[2]);

            double avgTemp = sumTemp / count;
            double avgHumidity = sumHumidity / count;

            return avgTemp + "," + avgHumidity;
        });

        // 5 Écrire les résultats dans le topic station-averages
        averageTable.toStream().to("station-averages", Produced.with(Serdes.String(), Serdes.String()));


        // --- Démarrer l'application Kafka Streams
        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
            System.out.println("\n--- Application Kafka Streams DÉMARRÉE. En attente de messages sur 'weather-data'...");
            System.out.println("    Appuyez sur Ctrl+C pour arrêter proprement.");
            Thread.currentThread().join();
            // Ajouter un hook pour arrêter proprement l'application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        } catch (Exception e) {
            System.err.println("Erreur lors du démarrage de Kafka Streams: " + e.getMessage());
        }

    }
}
