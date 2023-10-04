package org.example;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.example.resources.Queue;
import org.example.resources.QueueGroup;
import org.example.resources.QueueStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@ControllerConfiguration
public class QueueReconciler implements Reconciler<Queue>, Cleaner<Queue>, EventSourceInitializer<Queue> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueReconciler.class);

    @Override
    public UpdateControl<Queue> reconcile(Queue queue, Context<Queue> context) throws InterruptedException {
        LOGGER.info("Reconciling queue: {}", queue.getMetadata().getName());

        var queueGroups = context.getSecondaryResources(QueueGroup.class);
        var queueGroupNames = queueGroups.stream().map(QueueGroup::getMetadata).map(ObjectMeta::getName).collect(Collectors.toSet());
        LOGGER.info("Secondary resources for queue {}: {}", queue.getMetadata().getName(), queueGroupNames);
        var expectedQueueGroupNames = queue.getSpec().getGroups();
        if ((queueGroups.size() != queue.getSpec().getGroups().size() ||
                !queueGroupNames.containsAll(expectedQueueGroupNames))
                && !queue.isMarkedForDeletion()) {
            throw new RuntimeException("Queue %s refers to unknown queue group(s): %s not matching %s".formatted(
                    queue.getMetadata().getName(), expectedQueueGroupNames, queueGroupNames));
        }

        var allQueueGroupsReconciled = queueGroups.stream().allMatch(this::isQueueGroupReady);

        if (allQueueGroupsReconciled) {
            LOGGER.info("Start reconciliation queue: {}", queue.getMetadata().getName());
            Thread.sleep((long) (Math.random() * 10000));
            LOGGER.info("Reconciled queue: {}", queue.getMetadata().getName());
            var status = new QueueStatus();
            status.setMessage("Queue %s reconciled".formatted(queue.getMetadata().getName()));
            queue.setStatus(status);
            return UpdateControl.updateStatus(queue);
        } else {
            LOGGER.info("Queue groups still pending reconciliation for {}: {}", queue.getMetadata().getName(),
                    queueGroups.stream().filter(Predicate.not(this::isQueueGroupReady)).map(QueueGroup::getMetadata).map(ObjectMeta::getName).collect(Collectors.joining(",")));
            return UpdateControl.noUpdate();
        }
    }

    @Override
    public DeleteControl cleanup(Queue queue, Context<Queue> context) {
        LOGGER.info("Deleting queue: {}", queue.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }

    private boolean isQueueGroupReady(QueueGroup queueGroup) {
        return queueGroup.getStatus() != null && queueGroup.getStatus().getMessage() != null;
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<Queue> context) {
        final SecondaryToPrimaryMapper<QueueGroup> matchingGroups =
                (QueueGroup g) -> context.getPrimaryCache()
                        .list(queue -> queue.getSpec().getGroups().stream().anyMatch(
                                q -> q.equals(g.getMetadata().getName()))
                        )
                        .map(ResourceID::fromResource)
                        .collect(Collectors.toSet());

        InformerConfiguration<QueueGroup> configuration =
                InformerConfiguration.from(QueueGroup.class, context)
                        .withSecondaryToPrimaryMapper(matchingGroups)
                        .withPrimaryToSecondaryMapper(
                                (Queue primary) -> primary.getSpec().getGroups().stream()
                                        .map(group -> new ResourceID(group,
                                                primary.getMetadata().getNamespace()))
                                        .collect(Collectors.toSet())
                        )
                        .build();
        return EventSourceInitializer
                .nameEventSources(new InformerEventSource<>(configuration, context));
    }
}
