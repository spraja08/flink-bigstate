package com.amazonaws.streams.utils;

public class Event {
    private String event;

    public Event(String event) {
        this.event = event;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }
}
