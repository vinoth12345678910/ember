package com.ember.chaos;

/**
 * How an operation ended, from the client's point of view.
 * OK   - acknowledged: the system promised it is stored.
 * FAIL - definitely NOT applied (it was refused before it could be stored).
 * INFO - unknown: a timeout or a lost connection; the effect may or may not exist.
 */
public enum Outcome {
    OK, FAIL, INFO
}
