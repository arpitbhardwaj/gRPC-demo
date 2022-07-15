package com.ab.greeting.client;

import com.proto.greeting.GreetingRequest;
import com.proto.greeting.GreetingResponse;
import com.proto.greeting.GreetingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GreetingClient {
    public static void main(String[] args) throws InterruptedException {
        if (args.length == 0){
            System.out.println("Need one argument to proceed");
        }
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        switch (args[0]){
            case "greet":
                doGreet(channel);
                break;
            case "greetManyTimes":
                doGreetManyTimes(channel);
                break;
            case "longGreet":
                doLongGreet(channel);
                break;
            case "greetEveryone":
                doGreetEveryone(channel);
                break;
            default:
                System.out.println("Invalid argument");
        }
        System.out.println("Shutting Down");
        channel.shutdown();
    }

    private static void doGreetEveryone(ManagedChannel channel) throws InterruptedException {
        GreetingServiceGrpc.GreetingServiceStub stub = GreetingServiceGrpc.newStub(channel); //asynchronous
        List<String> names = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Collections.addAll(names, "Arpit", "PK", "Manav");

        StreamObserver<GreetingRequest> stream = stub.greetEveryone(new StreamObserver<GreetingResponse>() {
            @Override
            public void onNext(GreetingResponse response) {
                System.out.println(response.getResult());
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        for (String name:
                names) {
            stream.onNext(GreetingRequest.newBuilder().setFirstName(name).build());
        }

        stream.onCompleted();
        latch.await(10, TimeUnit.SECONDS);
    }

    private static void doLongGreet(ManagedChannel channel) throws InterruptedException {
        GreetingServiceGrpc.GreetingServiceStub stub = GreetingServiceGrpc.newStub(channel); //asynchronous
        List<String> names = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Collections.addAll(names, "Arpit", "PK", "Manav");

        StreamObserver<GreetingRequest> stream = stub.longGreet(new StreamObserver<GreetingResponse>() {
            @Override
            public void onNext(GreetingResponse response) {
                System.out.println(response.getResult());
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        for (String name:
             names) {
            stream.onNext(GreetingRequest.newBuilder().setFirstName(name).build());
        }

        stream.onCompleted();
        latch.await(10, TimeUnit.SECONDS);
    }

    private static void doGreetManyTimes(ManagedChannel channel) {
        GreetingServiceGrpc.GreetingServiceBlockingStub stub = GreetingServiceGrpc.newBlockingStub(channel);
        stub.greetManyTimes(GreetingRequest.newBuilder().setFirstName("Arpit").build()).forEachRemaining(response -> {
            System.out.println("Greeting: " + response.getResult());
        });

    }

    private static void doGreet(ManagedChannel channel) {
        GreetingServiceGrpc.GreetingServiceBlockingStub stub = GreetingServiceGrpc.newBlockingStub(channel);
        GreetingResponse response = stub.greet(GreetingRequest.newBuilder().setFirstName("Arpit").build());
        System.out.println("Greeting: " + response.getResult());
    }
}
