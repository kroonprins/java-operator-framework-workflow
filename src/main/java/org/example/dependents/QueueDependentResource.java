package org.example.dependents;

import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.AbstractEventSourceHolderDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.example.resources.Queue;
import org.example.resources.QueueStatus;
import org.example.resources.Subscription;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueueDependentResource
        extends AbstractEventSourceHolderDependentResource<Queue, Subscription, InformerEventSource<Queue, Subscription>>
        implements Condition<Queue, Subscription> {
    public QueueDependentResource() {
        super(Queue.class);
    }

    @Override
    protected InformerEventSource<Queue, Subscription> createEventSource(EventSourceContext<Subscription> context) {
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
        return new InformerEventSource<>(configuration, context);
    }

    @Override
    public boolean isMet(DependentResource<Queue, Subscription> dependentResource, Subscription primary, Context<Subscription> context) {
        return dependentResource.getSecondaryResource(primary, context)
                .map(Queue::getStatus)
                .map(QueueStatus::getMessage)
                .filter(Predicate.not(String::isBlank))
                .isPresent();
    }
}
