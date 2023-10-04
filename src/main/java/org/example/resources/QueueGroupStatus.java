package org.example.resources;

public class QueueGroupStatus {
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "QueueGroupStatus{" +
                "message='" + message + '\'' +
                '}';
    }
}
