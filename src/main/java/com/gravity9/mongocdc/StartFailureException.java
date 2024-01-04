package com.gravity9.mongocdc;

public class StartFailureException extends BaseMongoCDCException {
    public StartFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public static StartFailureException startFailure(Throwable cause) {
        return new StartFailureException("Start failed", cause);
    }
}
