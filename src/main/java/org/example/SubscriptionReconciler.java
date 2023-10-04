package org.example;

import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.dependent.workflow.WorkflowReconcileResult;
import org.example.dependents.QueueDependentResource;
import org.example.resources.Subscription;
import org.example.resources.SubscriptionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ControllerConfiguration(
        dependents = {
                @Dependent(type = QueueDependentResource.class, readyPostcondition = QueueDependentResource.class)
        }
)
public class SubscriptionReconciler implements Reconciler<Subscription>, Cleaner<Subscription> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionReconciler.class);

    @Override
    public UpdateControl<Subscription> reconcile(Subscription subscription, Context<Subscription> context) {
        LOGGER.info("Reconciling subscription: {}", subscription.getMetadata().getName());

        var allDependentResourcesReady = context.managedDependentResourceContext().getWorkflowReconcileResult()
                .map(WorkflowReconcileResult::allDependentResourcesReady).orElseThrow();
        if (!allDependentResourcesReady) {
            LOGGER.info("Not all dependents ready for subscription: {}", subscription.getMetadata().getName());
            return UpdateControl.noUpdate();
        }

        LOGGER.info("Reconciled subscription: {}", subscription.getMetadata().getName());
        var status = new SubscriptionStatus();
        status.setMessage("Subscription %s reconciled".formatted(subscription.getMetadata().getName()));
        subscription.setStatus(status);
        return UpdateControl.updateStatus(subscription);
    }

    @Override
    public DeleteControl cleanup(Subscription subscription, Context<Subscription> context) {
        LOGGER.info("Deleting subscription: {}", subscription.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }
}
