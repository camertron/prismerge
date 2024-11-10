use std::time::Duration;

pub fn format_duration(duration: &Duration) -> String {
    let total_secs = duration.as_secs();
    let hrs = total_secs / 3_600;
    let mins = (total_secs % 3_600) / 60;
    let secs = (total_secs % 3_600) % 60;

    if hrs > 0 {
        format!("{}h{:02}m{:02}s", hrs, mins, secs)
    } else {
        format!("{}m{:02}s", mins, secs)
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::*;
    use std::time::Duration;

    #[test]
    fn format_duration_seconds() {
        assert!(format_duration(&Duration::from_secs(5)) == "0m05s");
        assert!(format_duration(&Duration::from_secs(59)) == "0m59s");
    }

    #[test]
    fn format_duration_minutes() {
        assert!(format_duration(&Duration::from_secs(60)) == "1m00s");
        assert!(format_duration(&Duration::from_secs(61)) == "1m01s");
        assert!(format_duration(&Duration::from_secs(119)) == "1m59s");
    }

    #[test]
    fn format_duration_hours() {
        assert!(format_duration(&Duration::from_secs(3600)) == "1h00m00s");
        assert!(format_duration(&Duration::from_secs(3601)) == "1h00m01s");
        assert!(format_duration(&Duration::from_secs(3660)) == "1h01m00s");
        assert!(format_duration(&Duration::from_secs(3659)) == "1h00m59s");
        assert!(format_duration(&Duration::from_secs(3661)) == "1h01m01s");
        assert!(format_duration(&Duration::from_secs(4200)) == "1h10m00s");
        assert!(format_duration(&Duration::from_secs(4201)) == "1h10m01s");
        assert!(format_duration(&Duration::from_secs(4259)) == "1h10m59s");
    }
}
