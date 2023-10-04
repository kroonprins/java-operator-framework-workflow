package org.example;

import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.example.resources.Queue;
import org.example.resources.Subscription;
import org.example.resources.SubscriptionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ControllerConfiguration
public class SubscriptionReconciler implements Reconciler<Subscription>, Cleaner<Subscription>, EventSourceInitializer<Subscription> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionReconciler.class);

    @Override
    public UpdateControl<Subscription> reconcile(Subscription subscription, Context<Subscription> context) {
        LOGGER.info("Reconciling subscription: {}", subscription.getMetadata().getName());

        var queue = context.getSecondaryResource(Queue.class)
                .orElseThrow(() -> new RuntimeException("Subscription %s refers to unknown queue: %s".formatted(
                        subscription.getMetadata().getName(), subscription.getSpec().getQueue())));
        var queueName = queue.getMetadata().getName();
        LOGGER.info("Secondary resource for subscription {}: {}", subscription.getMetadata().getName(), queueName);

        var isQueueReconciled = isQueueReady(queue);

        if (isQueueReconciled) {
            LOGGER.info("Reconciled subscription: {}", subscription.getMetadata().getName());
            var status = new SubscriptionStatus();
            status.setMessage("Subscription %s reconciled".formatted(subscription.getMetadata().getName()));
            subscription.setStatus(status);
            return UpdateControl.updateStatus(subscription);
        } else {
            LOGGER.info("Queue still pending reconciliation for {}: {}", queue.getMetadata().getName(), queueName);
            return UpdateControl.noUpdate();
        }
    }

    @Override
    public DeleteControl cleanup(Subscription subscription, Context<Subscription> context) {
        LOGGER.info("Deleting subscription: {}", subscription.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }

    private boolean isQueueReady(Queue queue) {
        return queue.getStatus() != null && queue.getStatus().getMessage() != null;
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<Subscription> context) {
        final SecondaryToPrimaryMapper<Queue> matchingQueue =
                (Queue q) -> context.getPrimaryCache()
                        .list(subscription -> subscription.getSpec().getQueue().equals(q.getMetadata().getName())
                        )
                        .map(ResourceID::fromResource)
                        .collect(Collectors.toSet());

        InformerConfiguration<Queue> configuration =
                InformerConfiguration.from(Queue.class, context)
                        .withSecondaryToPrimaryMapper(matchingQueue)
                        .withPrimaryToSecondaryMapper(
                                (Subscription primary) -> Set.of(new ResourceID(primary.getSpec().getQueue(),
                                        primary.getMetadata().getNamespace()))
                        )
                        .build();
        return EventSourceInitializer
                .nameEventSources(new InformerEventSource<>(configuration, context));
    }
}
