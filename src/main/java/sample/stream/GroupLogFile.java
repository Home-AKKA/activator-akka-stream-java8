package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.javadsl.*;
import akka.util.ByteString;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GroupLogFile {

  private final static                                ActorSystem SYSTEM = ActorSystem.create("Sys");
  private final static                    ActorMaterializer MATERIALIZER = ActorMaterializer.create(SYSTEM);
  // (Flow - это наш промежуточный-поток, который лежит между источником и обработчиком И этот промежуточный-поток что-то делает еще...)
  private final static Flow<ByteString, ByteString, NotUsed> SPLIT_LINES_FLOW = Framing.delimiter(ByteString.fromString("\n"), 256, FramingTruncation.ALLOW);

  public static void main(String... args) throws IOException {

    final Pattern loglevelPattern = Pattern.compile(".*\\[(DEBUG|INFO|WARN|ERROR)\\].*");

    // read lines from a log file
    final String inPath = "src/main/resources/logfile.txt";
    final   File inFile = new File(inPath);

    final Map<String, PrintWriter> outputs = new HashMap<>(); // пишет в файл...
    for (String loglevel : Arrays.asList("DEBUG", "INFO", "WARN", "ERROR", "UNKNOWN")) {
      final String outPath = "target/" + loglevel + ".txt";
      final PrintWriter output = new PrintWriter(new FileOutputStream(outPath), true);
      outputs.put(loglevel, output);
    }

    FileIO.fromFile(inFile)  // (это наш эмитатор, который дает-инициирует команды...) Source - это наш эмитатор...
            .via(SPLIT_LINES_FLOW) // разобрать байты (фрагменты данных) на строки. Flow - дещо, з тільки рівно одним входом та виходом
            .map(ByteString::utf8String)
            .map(line -> { // group them by log level
              final Matcher matcher = loglevelPattern.matcher(line);
              if (matcher.find()) {
                return new Pair<>(matcher.group(1), line);
              } else {
                return new Pair<>("UNKNOWN", line);
              } })
            .runForeach(pair -> { outputs.get(pair.first()).println(pair.second()); }, MATERIALIZER) // писать строки каждой группы в отдельный файл (это наш обработчик, который слушает ивенты-команды и что-то делает...)
            .handle((ignored, failure) -> {
              outputs.forEach((key, writer) -> { writer.close(); });
              SYSTEM.terminate();
              return NotUsed.getInstance();
            });

  }
}
