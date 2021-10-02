package com.pulot;

public class PersonResponse {
    private String message;

    public PersonResponse() {
    }

    public PersonResponse(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
