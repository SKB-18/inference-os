use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    pub state: CircuitState,
    pub failure_threshold: u32,
    pub consecutive_failures: u32,
    pub cooldown_duration: Duration,
    pub last_state_change: Instant,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(3, Duration::from_secs(10))
    }
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown_duration: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_threshold,
            consecutive_failures: 0,
            cooldown_duration,
            last_state_change: Instant::now(),
        }
    }

    pub fn can_execute(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if self.last_state_change.elapsed() >= self.cooldown_duration {
                    self.state = CircuitState::HalfOpen;
                    self.last_state_change = Instant::now();
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub fn on_success(&mut self) {
        self.consecutive_failures = 0;
        self.state = CircuitState::Closed;
        self.last_state_change = Instant::now();
    }

    pub fn on_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.failure_threshold || self.state == CircuitState::HalfOpen {
            self.state = CircuitState::Open;
            self.last_state_change = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_trips_to_open() {
        let mut cb = CircuitBreaker::new(2, Duration::from_millis(50));
        assert_eq!(cb.state, CircuitState::Closed);
        assert!(cb.can_execute());

        cb.on_failure();
        assert_eq!(cb.state, CircuitState::Closed);
        assert!(cb.can_execute());

        cb.on_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert!(!cb.can_execute());
    }

    #[test]
    fn test_circuit_breaker_half_open_recovery() {
        let mut cb = CircuitBreaker::new(1, Duration::from_millis(10));
        cb.on_failure();
        assert_eq!(cb.state, CircuitState::Open);

        std::thread::sleep(Duration::from_millis(15));
        assert!(cb.can_execute());
        assert_eq!(cb.state, CircuitState::HalfOpen);

        cb.on_success();
        assert_eq!(cb.state, CircuitState::Closed);
        assert_eq!(cb.consecutive_failures, 0);
    }

    #[test]
    fn test_circuit_breaker_half_open_failure_reopens() {
        let mut cb = CircuitBreaker::new(1, Duration::from_millis(10));
        cb.on_failure();
        assert_eq!(cb.state, CircuitState::Open);

        std::thread::sleep(Duration::from_millis(15));
        assert!(cb.can_execute());
        assert_eq!(cb.state, CircuitState::HalfOpen);

        cb.on_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert!(!cb.can_execute());
    }
}

