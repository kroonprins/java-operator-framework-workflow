package org.example;

import io.javaoperatorsdk.operator.Operator;
import org.takes.facets.fork.FkRegex;
import org.takes.facets.fork.TkFork;
import org.takes.http.Exit;
import org.takes.http.FtBasic;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        Operator operator = new Operator();
        operator.register(new QueueGroupReconciler());
        operator.register(new QueueReconciler());
        operator.register(new SubscriptionReconciler());
        operator.start();

        new FtBasic(new TkFork(new FkRegex("/health", "ALL GOOD.")), 8080).start(Exit.NEVER);
    }
}
