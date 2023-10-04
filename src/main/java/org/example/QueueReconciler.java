package org.example;

import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.dependent.workflow.WorkflowReconcileResult;
import org.example.dependents.QueueGroupDependentResource;
import org.example.resources.Queue;
import org.example.resources.QueueStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ControllerConfiguration(
        dependents = {
                @Dependent(type = QueueGroupDependentResource.class, readyPostcondition = QueueGroupDependentResource.class)
        }
)
public class QueueReconciler implements Reconciler<Queue>, Cleaner<Queue> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueReconciler.class);

    @Override
    public UpdateControl<Queue> reconcile(Queue queue, Context<Queue> context) throws InterruptedException {
        LOGGER.info("Reconciling queue: {}", queue.getMetadata().getName());

        var allDependentResourcesReady = context.managedDependentResourceContext().getWorkflowReconcileResult()
                .map(WorkflowReconcileResult::allDependentResourcesReady).orElseThrow();
        if (!allDependentResourcesReady) {
            LOGGER.info("Not all dependents ready for queue: {}", queue.getMetadata().getName());
            return UpdateControl.noUpdate();
        }

        LOGGER.info("Start reconciliation queue: {}", queue.getMetadata().getName());
        Thread.sleep((long) (Math.random() * 10000));
        LOGGER.info("Reconciled queue: {}", queue.getMetadata().getName());
        var status = new QueueStatus();
        status.setMessage("Queue %s reconciled".formatted(queue.getMetadata().getName()));
        queue.setStatus(status);
        return UpdateControl.updateStatus(queue);
    }

    @Override
    public DeleteControl cleanup(Queue queue, Context<Queue> context) {
        LOGGER.info("Deleting queue: {}", queue.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }
}
