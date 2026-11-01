package io.governance.model;

import java.time.Instant;

public record LoginBurst(
        String firstEventId,
        String thresholdEventId,
        Instant thresholdEventTime,
        String lastEventId,
        Instant lastEventTime,
        long count
) {
    public static LoginBurst add(LoginBurst current, GovernanceEvent event) {
        if (current == null || current.count() == 0) {
            return new LoginBurst(event.eventId(), null, null, event.eventId(), event.eventTime(), 1);
        }
        long nextCount = current.count() + 1;
        String thresholdId = nextCount == 5 ? event.eventId() : current.thresholdEventId();
        Instant thresholdTime = nextCount == 5 ? event.eventTime() : current.thresholdEventTime();
        return new LoginBurst(
                current.firstEventId(), thresholdId, thresholdTime,
                event.eventId(), event.eventTime(), nextCount
        );
    }
}
