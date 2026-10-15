//! Agent + loop metrics as pure functions.
//!
//! The same logic runs in-process (batch jobs, tests) and inside Langfuse
//! code evaluators (which are free and deterministic, unlike LLM judges).
//! Score names match `langfuse/score-configs/*.json`. All functions are
//! total: degenerate inputs yield the conservative value, never NaN.

/// Whether an agent run exceeded its step budget (`trajectory_steps`).
pub fn trajectory_over_budget(steps: u64, budget: u64) -> bool {
    steps > budget
}

/// Fraction of step budget remaining, clamped to [0, 1]. A run exactly on
/// budget scores 0; fewer steps score higher (cheaper runs win ties).
pub fn trajectory_headroom(steps: u64, budget: u64) -> f64 {
    if budget == 0 {
        return 0.0;
    }
    (1.0 - steps as f64 / budget as f64).clamp(0.0, 1.0)
}

/// Whether every required tool was called at least once, in any order.
pub fn tool_calls_complete(called: &[&str], required: &[&str]) -> bool {
    required.iter().all(|r| called.contains(r))
}

/// Tool error rate in [0, 1]; zero calls is 0 (nothing failed), never NaN.
pub fn tool_error_rate(errors: u64, calls: u64) -> f64 {
    if calls == 0 {
        return 0.0;
    }
    (errors as f64 / calls as f64).clamp(0.0, 1.0)
}

/// Session outcome label for `session_resolution`: resolved beats escalated
/// beats abandoned. Matches the score-config categories exactly.
pub fn session_outcome(completed: bool, escalated: bool) -> &'static str {
    if completed {
        "resolved"
    } else if escalated {
        "escalated"
    } else {
        "abandoned"
    }
}

/// Share of served enrichments from each personalization tier. Inputs are
/// raw counts; output sums to 1 (or all zeros when nothing was served).
pub fn blend_hit_rate(personal: u64, cohort: u64, global: u64) -> (f64, f64, f64) {
    let total = personal + cohort + global;
    if total == 0 {
        return (0.0, 0.0, 0.0);
    }
    let total = total as f64;
    (
        personal as f64 / total,
        cohort as f64 / total,
        global as f64 / total,
    )
}

/// Personalized outcome rate minus cohort-default rate over the same window.
/// Positive means the blend earns its complexity; near zero means simplify.
pub fn personalization_lift(personal_rate: f64, cohort_rate: f64) -> f64 {
    personal_rate - cohort_rate
}

/// Absolute gap between predicted score and realized outcome rate.
/// The calibration alert fires when this exceeds its threshold.
pub fn calibration_gap(predicted: f64, realized: f64) -> f64 {
    (predicted - realized).abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_boundaries() {
        assert!(!trajectory_over_budget(10, 10));
        assert!(trajectory_over_budget(11, 10));
        assert_eq!(trajectory_headroom(5, 10), 0.5);
        assert_eq!(trajectory_headroom(10, 10), 0.0);
        assert_eq!(trajectory_headroom(12, 10), 0.0);
        assert_eq!(trajectory_headroom(0, 0), 0.0);
    }

    #[test]
    fn tool_call_checks() {
        assert!(tool_calls_complete(&["search", "read"], &["search"]));
        assert!(tool_calls_complete(&[], &[]));
        assert!(!tool_calls_complete(&["read"], &["search"]));
        assert_eq!(tool_error_rate(1, 4), 0.25);
        assert_eq!(tool_error_rate(0, 0), 0.0);
        assert_eq!(tool_error_rate(9, 2), 1.0);
    }

    #[test]
    fn session_labels_match_score_config() {
        assert_eq!(session_outcome(true, false), "resolved");
        assert_eq!(session_outcome(true, true), "resolved");
        assert_eq!(session_outcome(false, true), "escalated");
        assert_eq!(session_outcome(false, false), "abandoned");
    }

    #[test]
    fn blend_shares_sum_to_one() {
        let (p, c, g) = blend_hit_rate(50, 35, 15);
        assert!((p + c + g - 1.0).abs() < 1e-12);
        assert_eq!(blend_hit_rate(0, 0, 0), (0.0, 0.0, 0.0));
    }

    #[test]
    fn lift_and_calibration_signs() {
        assert!(personalization_lift(0.62, 0.55) > 0.0);
        assert!(personalization_lift(0.50, 0.55) < 0.0);
        assert!((calibration_gap(0.6, 0.55) - 0.05).abs() < 1e-12);
        assert_eq!(calibration_gap(0.5, 0.5), 0.0);
    }
}
