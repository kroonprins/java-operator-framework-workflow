package org.example.resources;

// TODO can use records for these?
public class SubscriptionSpec {
    private String queue;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @Override
    public String toString() {
        return "SubscriptionSpec{" +
                "queue='" + queue + '\'' +
                '}';
    }
}
