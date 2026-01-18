package com.smith.processor.model;


public enum EventType {
    USER_LOGIN,
    USER_LOGOUT,
    USER_FAILED_LOGIN,
    USER_MULTIPLE_FAILED_LOGIN,
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    ORDER_PLACED,
    ORDER_CANCELLED,
}
