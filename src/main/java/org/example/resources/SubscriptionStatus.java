package org.example.resources;

public class SubscriptionStatus {
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "QueueStatus{" +
                "message='" + message + '\'' +
                '}';
    }
}
