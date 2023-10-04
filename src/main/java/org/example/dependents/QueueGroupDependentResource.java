package org.example.dependents;

import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.AbstractEventSourceHolderDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.example.resources.Queue;
import org.example.resources.QueueGroup;
import org.example.resources.QueueGroupStatus;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueueGroupDependentResource
        extends AbstractEventSourceHolderDependentResource<QueueGroup, Queue, InformerEventSource<QueueGroup, Queue>>
        implements BulkDependentResource<QueueGroup, Queue>, Condition<QueueGroup, Queue> {

    public QueueGroupDependentResource() {
        super(QueueGroup.class);
    }

    @Override
    protected InformerEventSource<QueueGroup, Queue> createEventSource(EventSourceContext<Queue> context) {
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
        return new InformerEventSource<>(configuration, context);
    }

    @Override
    public Map<String, QueueGroup> desiredResources(Queue primary, Context<Queue> context) {
        return getSecondaryResources(primary, context);
    }

    @Override
    public Map<String, QueueGroup> getSecondaryResources(Queue primary, Context<Queue> context) {
        return context.getSecondaryResourcesAsStream(QueueGroup.class)
                .collect(Collectors.toMap(g -> g.getMetadata().getName(), Function.identity(), (k1, k2) -> k1));
    }

    @Override
    public void deleteTargetResource(Queue primary, QueueGroup resource, String key, Context<Queue> context) {
        // noop
    }

    @Override
    public QueueGroup create(QueueGroup desired, Queue primary, Context<Queue> context) {
        // noop
        return null;
    }

    @Override
    public boolean isMet(DependentResource<QueueGroup, Queue> dependentResource, Queue primary, Context<Queue> context) {
        return context.getSecondaryResourcesAsStream(QueueGroup.class)
                .allMatch(queueGroup ->
                        Optional.ofNullable(queueGroup)
                                .map(QueueGroup::getStatus)
                                .map(QueueGroupStatus::getMessage)
                                .filter(Predicate.not(String::isBlank))
                                .isPresent()
                );
    }
}
