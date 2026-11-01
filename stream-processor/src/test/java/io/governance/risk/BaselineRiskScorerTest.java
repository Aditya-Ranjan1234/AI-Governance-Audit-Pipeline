package io.governance.risk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BaselineRiskScorerTest {
    @Test void usesSeverityTimesFrequencyBand() {
        assertEquals(16.0, BaselineRiskScorer.score(4, 1));
        assertEquals(32.0, BaselineRiskScorer.score(4, 3));
        assertEquals(100.0, BaselineRiskScorer.score(5, 99));
    }

    @Test void bucketsFrequencyDeterministically() {
        assertEquals(1, BaselineRiskScorer.frequencyBand(1));
        assertEquals(2, BaselineRiskScorer.frequencyBand(3));
        assertEquals(3, BaselineRiskScorer.frequencyBand(7));
        assertEquals(4, BaselineRiskScorer.frequencyBand(15));
        assertEquals(5, BaselineRiskScorer.frequencyBand(16));
    }
}

