package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import scala.concurrent.forkjoin.ThreadLocalRandom;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;


public class WritePrimes {

  private static final             ActorSystem SYSTEM = ActorSystem.create("Sys");
  private static final ActorMaterializer MATERIALIZER = ActorMaterializer.create(SYSTEM);
  // (Source - это наш эмитатор, который дает-инициирует команды...) - дещо, з тільки одним вихідним потоком
  private static final Source<Integer, NotUsed>                   PRIME_SOURCE = Source.from(new RandomIterable(1000000)) // generate random numbers
          .filter(WritePrimes::isPrime)            // filter prime numbers
          .filter(prime -> isPrime(prime + 2))  // and neighbor +2 is also prime
          .map(t -> { System.out.println(">>>>>>>>>>>>>>> PRIME_SOURCE = Source.from"); return t; });

  // Sink - дещо, з тільки одним вхідним потоком
  private static final Sink<ByteString, CompletionStage<IOResult>> OUTPUT_SINK = FileIO.toFile(new File("target/primes.txt")); // write to file sink
  private static final       Sink<Integer, CompletionStage<Done>> CONSOLE_SINK = Sink.foreach(i -> {                                     // console output sink
    System.out.println(">>>>>>>>>>>>>>> CONSOLE_SINK = Sink.foreach");
    System.out.println(i);
  } );
  private static final          Sink<Integer, CompletionStage<Done>> SOME_SINK = Sink.foreach(x -> System.out.println(">>>>>>>>>>>>>>>") );
  private static final  Sink<Integer, CompletionStage<IOResult>> _GENERAL_SINK = Flow.of(Integer.class) // Flow - дещо, з тільки рівно одним входом та виходом
          .map(i -> {
            System.out.println(">>>>>>>>>>>>>>> _GENERAL_SINK = Flow.of");
            Thread.sleep(50);                //...
            return ByteString.fromString(i.toString()); })
          .toMat(OUTPUT_SINK, Keep.right()); // ми з'єднаємо Flow до попередньо підготованого Sink з використанням toMat.

  // Graph - запакована топологія обробки потоку, що показує певний набір вхідних та вихідних портів...
  private static final Graph<ClosedShape, CompletionStage<IOResult>> GRAPH = GraphDSL.create(_GENERAL_SINK, (builder, generalSink) -> { // GraphDSL - неявний побудовник графів
    System.out.println(">>>>>>>>>>>>>>> GRAPH = GraphDSL.create");

    UniformFanOutShape<Integer, Integer> BROADCAST = builder.add(Broadcast.create(3)); // Broadcast must have one or more output ports
    SourceShape<Integer>               primeSource = builder.add(PRIME_SOURCE);
    SinkShape<Integer>                 consoleSink = builder.add(CONSOLE_SINK);
    SinkShape<Integer>                    someSink = builder.add(SOME_SINK);

//    builder.from(builder.add(PRIME_SOURCE)).viaFanOut(BROADCAST).to(sink)
//            .from(BROADCAST).to(builder.add(CONSOLE_SINK));
    builder.from(primeSource).viaFanOut(BROADCAST); // эта операция очень важна - она вещает данные от источника (no more outlets free on akka.stream.UniformFanOutShape...)
    builder.from(BROADCAST).to(generalSink);        // ...на (конечный) обработчик-1, который указан в нашем броадкасте
    builder.from(BROADCAST).to(consoleSink);        // ...на (конечный) обработчик-2, который указан в нашем броадкасте
    builder.from(BROADCAST).to(someSink);


    return ClosedShape.getInstance();
  });


  public static void main(String... args) {

    // Виконуваний граф (RunnableGraph) - використовує неявний ActorMaterializer для матеріалізації та виконання Flow.
    final CompletionStage<IOResult> future = RunnableGraph.fromGraph(GRAPH).run(MATERIALIZER);

    future.handle((ioResult, failure) -> {
//      if (failure != null) System.err.println("Failure: " + failure);
//      else if (!ioResult.wasSuccessful()) System.err.println("Writing to file failed " + ioResult.getError());
//      else System.out.println("Successfully wrote " + ioResult.getCount() + " bytes");
      SYSTEM.terminate();
      return NotUsed.getInstance();
    });
  }

  private static boolean isPrime(int n) {
//    if (n <= 1)
//      return false;
//    else if (n == 2)
//      return true;
//    else {
//      for (int i = 2; i < n; i++) {
//        if (n % i == 0)
//          return false;
//      }
//      return true;
//    }
    if (n <= 9999)
      return false;
    return true;
  }

  private static class RandomIterable implements Iterable<Integer> {

    private final int maxRandomNumberSize;

    RandomIterable(int maxRandomNumberSize) {
      this.maxRandomNumberSize = maxRandomNumberSize;
    }

    @Override
    public Iterator<Integer> iterator() { //TODO
      return new Iterator<Integer>() {
        private Integer _next = 10010;

        @Override
        public boolean hasNext() {
          System.out.println("========= " + _next);
          if (_next <= 9999)
            return false;
          return true;
        }

        @Override
        public Integer next() {
//          _next = ThreadLocalRandom.current().nextInt(maxRandomNumberSize);
          _next--;
          return _next;
        }
      };
    }
  }


}

