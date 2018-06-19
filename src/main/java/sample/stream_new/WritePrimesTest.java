package sample.stream_new;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.util.ByteString;

import java.io.*;
import java.time.LocalDate;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WritePrimesTest {

  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_BLACK = "\u001B[30m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_GREEN = "\u001B[32m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_BLUE = "\u001B[34m";
  private static final String ANSI_PURPLE = "\u001B[35m";
  private static final String ANSI_CYAN = "\u001B[36m";
  private static final String ANSI_WHITE = "\u001B[37m";
  private static final String ANSI_BLACK_BACKGROUND = "\u001B[40m";
  private static final String ANSI_RED_BACKGROUND = "\u001B[41m";
  private static final String ANSI_GREEN_BACKGROUND = "\u001B[42m";
  private static final String ANSI_GRAY_BACKGROUND = "\u001B[47m";
  private static final String ANSI_YELLOW_BACKGROUND = "\u001B[43m";
  private static final String ANSI_BLUE_BACKGROUND = "\u001B[44m";
  private static final String ANSI_PURPLE_BACKGROUND = "\u001B[45m";
  private static final String ANSI_CYAN_BACKGROUND = "\u001B[46m";
  private static final String ANSI_WHITE_BACKGROUND = "\u001B[47m";

  private static final String formatOutData = "|" + ANSI_BLUE + " %1$-10s " + ANSI_RESET + "|" + ANSI_CYAN + " %2$-50s " + ANSI_RESET + "|\n";






  private static final             ActorSystem SYSTEM = ActorSystem.create("sys");
  private static final ActorMaterializer MATERIALIZER = ActorMaterializer.create(SYSTEM);


  static Flow<ByteString, String, NotUsed> getDecodeFlow() {
    return Framing.delimiter(ByteString.fromString("\n"), 256, FramingTruncation.ALLOW)
            .map(byteString -> byteString.decodeString(ByteString.UTF_8()));
  }
  final static           String inPath = "src/main/resources/logfile.txt";
  final static             File inFile = new File(inPath);
  final static Pattern fileNamePattern = Pattern.compile(".*\\[(DEBUG|INFO|WARN|ERROR)\\].*");
  static String matchFileName(String data) {
    Matcher matcher = fileNamePattern.matcher(data);
    if (matcher.find())
      return matcher.group(1);
    else
      return "UNKNOWN";
  }

  /**
   * 2. (это промежуточный актор-обработчик №1)
   *    беру данные принятые из CSV-файла и разбиваю их по колонкам
   *    здесь-же эти данные из колонки конвертирую их в нужный мне формат для вещания
   *    и отправляю (передаю) эти данные куда-то дальше на обработку...
   *
   * Flow - дещо, з тільки рівно одним входом та виходом (приводим строки из файла к формату ByteString...) это промежуточное состояние
   */
  private final static Flow<ByteString, String, NotUsed> SPLIT_LINES_FLOW = getDecodeFlow();

  /**
   * 1. (собственно это мой источник `Emitter` №0)
   *    первым делом я принимаю какой-то CSV-файл
   *    и буду вещать-эмитировать эти данные куда-то дальше на обработку другому актору...
   *
   * (Source - это наш эмитатор, который дает-инициирует команды...) - дещо, з тільки одним вихідним потоком
   */
  private final static      Source<BroadcastDataDTO, CompletionStage<IOResult>> SOURCE = FileIO.fromFile(inFile) //TODO (должен идти перед flow)
          .via(SPLIT_LINES_FLOW)
          .map(data -> new BroadcastDataDTO(data, matchFileName(data)));

  private static final      Sink<BroadcastDataDTO, CompletionStage<Done>> CONSOLE_SINK = Sink.foreach(bCast -> {
    String[] out = new String[]{bCast.getFileName(), bCast.getData()};
    System.out.format(formatOutData, out);
  } );

  // Sink - дещо, з тільки одним вхідним потоком
  private static final   Sink<BroadcastDataDTO, CompletionStage<Done>> SMART_FILE_SINK = Sink.foreach(bCast -> {
    String outPath = "target/" + bCast.getFileName() + ".txt";
    try (FileWriter prWr = new FileWriter(new File(outPath), true)) {
      prWr.write(bCast.getData() + '\n');
    }
  });

  // (это один из конечных акторов-обработчиков,  я могу сделать его как по умолчанию - для того чтобы он мог например чтонибудь логировать...)
  private static final          Sink<BroadcastDataDTO, CompletionStage<Done>> LOG_SINK =
          Sink.foreach(bCast -> {
            try (FileWriter prWr = new FileWriter(new File("target/logfile.log"), true)) {
              prWr.write( LocalDate.now() + "   sample.stream_new.WritePrimesTest   " + bCast.getData() + '\n');
            }
          }); //TODO - ничего НЕдеалет..
//          Flow.of(BroadcastDataDTO.class).toMat(CONSOLE_SINK, Keep.right()); //TODO - перебрасывает действие на другой актор.. (и еще может что-то делать)

  // Graph - запакована топологія обробки потоку, що показує певний набір вхідних та вихідних портів...
  private static final Graph<ClosedShape, CompletionStage<Done>> GRAPH = GraphDSL.create(LOG_SINK, (builder, logSink) -> { // GraphDSL - неявний побудовник графів
    System.out.println(">>>>>>>>>>>>>>> GRAPH");

    //TODO важно - формат в котором будет выполняться обмен данными (вещание между акторами) должен быть ЕДИНЫМ!
    UniformFanOutShape<BroadcastDataDTO, BroadcastDataDTO> broadcast = builder.add(Broadcast.create(3)); // (сколько акторов я добавляю 'builder.add...')
    SourceShape<BroadcastDataDTO> source = builder.add(SOURCE);
    builder.from(source).viaFanOut(broadcast); // эта операция очень важна - она вещает данные от источника

    builder.from(broadcast).to(logSink);       // ...на обработчик-1 - логирование

    SinkShape<BroadcastDataDTO> consoleSink = builder.add(CONSOLE_SINK);
    builder.from(broadcast).to(consoleSink);   // ...на (конечный) обработчик-2 - красивый вывод на консоль

    SinkShape<BroadcastDataDTO> smartFileSink = builder.add(SMART_FILE_SINK);
    builder.from(broadcast).to(smartFileSink); // ...на (конечный) обработчик-3 - умная запись в файл

    return ClosedShape.getInstance();
  });


  public static void main(String... args) {

    // Виконуваний граф (RunnableGraph) - використовує неявний ActorMaterializer для матеріалізації та виконання Flow.
    final CompletionStage<Done> future = RunnableGraph.fromGraph(GRAPH).run(MATERIALIZER);

    future.handle((ignored, failure) -> {
//      if (failure != null) System.err.println("Failure: " + failure);
//      else if (!ignored.wasSuccessful()) System.err.println("Writing to file failed " + ignored.getError());
//      else System.out.println("Successfully wrote " + ignored.getCount() + " bytes");
      SYSTEM.terminate();
      return NotUsed.getInstance();
    });
  }


  // Формат в котором мы вещаем (broadcast...)
  private static class BroadcastDataDTO {
    private String data;
    private String fileName;

    BroadcastDataDTO() {

    }

    BroadcastDataDTO(String data) {
      this.data = data;
    }

    BroadcastDataDTO(String data, String fileName) {
      this.data = data;
      this.fileName = fileName;
    }

    public String getData() {
      return data;
    }

    public void setData(String data) {
      this.data = data;
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    @Override
    public String toString() {
      return "BroadcastDataDTO{" +
              "data='" + data + '\'' +
              ", fileName='" + fileName + '\'' +
              '}';
    }
  }
}
