package sample.stream;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import akka.NotUsed;
import scala.runtime.BoxedUnit;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;


public class BasicTransformation {

    private final static             ActorSystem SYSTEM = ActorSystem.create("Sys");
    private final static ActorMaterializer MATERIALIZER = ActorMaterializer.create(SYSTEM);

    public static void main(String... args) {

        String someText =
                "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, " +
                "when an unknown printer took a galley of type and scrambled it to make a type " +
                "specimen book.";

        final List<String> lSomeText = Arrays.asList(someText.split("\\s"));

        Source.from(lSomeText)                                 // (Source - это наш эмитатор, который дает-инициирует команды...)
                .map(e -> e.toUpperCase())                     // transform
                .runForeach(System.out::println, MATERIALIZER) // print to console (MATERIALIZER - это наш обработчик, который слушает ивенты-команды и что-то делает...)
                .handle((done, failure) -> {
                    SYSTEM.terminate();
                    return NotUsed.getInstance();
                });
    }
}
