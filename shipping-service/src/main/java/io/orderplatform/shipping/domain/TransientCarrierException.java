package io.orderplatform.shipping.domain;

public class TransientCarrierException extends RuntimeException {

    public TransientCarrierException(String message) {
        super(message);
    }
}
