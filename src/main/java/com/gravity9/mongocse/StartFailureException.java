package com.gravity9.mongocse;

public class StartFailureException extends BaseMongoCSEException {
    public StartFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public static StartFailureException startFailure(Throwable cause) {
        return new StartFailureException("Start failed", cause);
    }
}
