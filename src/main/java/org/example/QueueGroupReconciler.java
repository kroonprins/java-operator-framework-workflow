package org.example;

import io.javaoperatorsdk.operator.api.reconciler.*;
import org.example.resources.QueueGroup;
import org.example.resources.QueueGroupStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ControllerConfiguration
public class QueueGroupReconciler implements Reconciler<QueueGroup>, Cleaner<QueueGroup> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueGroupReconciler.class);

    @Override
    public UpdateControl<QueueGroup> reconcile(QueueGroup queueGroup, Context<QueueGroup> context) throws InterruptedException {
        LOGGER.info("Reconciling queue group: {}", queueGroup.getMetadata().getName());
        Thread.sleep((long) (Math.random() * 20000));
        LOGGER.info("Reconciled queue group: {}", queueGroup.getMetadata().getName());
        var status = new QueueGroupStatus();
        status.setMessage("Queue group %s reconciled".formatted(queueGroup.getMetadata().getName()));
        queueGroup.setStatus(status);
        return UpdateControl.updateStatus(queueGroup);
    }

    @Override
    public DeleteControl cleanup(QueueGroup queueGroup, Context<QueueGroup> context) {
        LOGGER.info("Deleting queue group: {}", queueGroup.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }
}
