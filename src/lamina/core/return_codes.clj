(ns lamina.core.return-codes)

(def error-code?
  #{:lamina/error!
    :lamina/deactivated!
    :lamina/closed!
    :lamina/drained!
    :lamina/timeout!
    :lamina/already-reaalized!
    :lamina/not-claimed!})

(def non-error-codes
  [:lamina/subscribed
   :lamina/deferred
   :lamina/realized
   :lamina/enqueued
   :lamina/suspended
   :lamina/grounded
   :lamina/false
   :lamina/filtered])

(defn error-code->exception [error]
  (case error
    :lamina/error!
    (IllegalStateException. "tried to enqueue into a channel in an error state")

    :lamina/deactivated!
    (IllegalStateException. "the client has been deactivated.")

    :lamina/closed!
    (IllegalStateException. "tried to enqueue into a close channel")

    :lamina/drained!
    (IllegalStateException. "cannot read from a drained channel")

    :lamina/timeout!
    (java.util.concurrent.TimeoutException.)

    :lamina/already-realized!
    (IllegalStateException. "result-channel has already been realized")

    :lamina/not-claimed!
    (IllegalStateException. "the result-channel has not been claimed (if you see this, let ztellman know).")

    nil))


