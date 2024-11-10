use indicatif::{ProgressBar, ProgressStyle};
use std::io::{self, IsTerminal};

pub enum ProgressType {
    Bar(ProgressBar),
    Console,
    Null
}

/* A wrapper around the ProgressBar crate that falls back to regular 'ol console logging
 * if STDIN isn't a terminal. Progress bars don't work super well in GitHub Actions and
 * end up writing nothing, making it difficult to track merge progress.
 */
pub struct ProgressIndicator {
    progress_type: ProgressType,
    model_name: String,
    total_rows: u64,
    count: u64
}

impl ProgressIndicator {
    pub fn new(model_name: &str, total_rows: u64) -> Self {
        if io::stdin().is_terminal() {
            let pb = ProgressBar::new(total_rows);

            pb.set_style(
                ProgressStyle::with_template(
                    format!("{{spinner:.green}} {} [{{elapsed_precise}}] [{{wide_bar:.cyan/blue}}] {{pos}}/{{len}}", model_name).as_str()
                )
                .unwrap()
                .progress_chars("#>-"));

            ProgressIndicator {
                progress_type: ProgressType::Bar(pb),
                model_name: model_name.to_string(),
                total_rows: total_rows,
                count: 0
            }
        } else {
            ProgressIndicator {
                progress_type: ProgressType::Console,
                model_name: model_name.to_string(),
                total_rows: total_rows,
                count: 0
            }
        }
    }

    pub fn null() -> Self {
        ProgressIndicator {
            progress_type: ProgressType::Null,
            model_name: "".to_string(),
            total_rows: 0,
            count: 0
        }
    }

    pub fn inc(self: &mut Self, delta: u64) {
        match &self.progress_type {
            ProgressType::Null => (),
            ProgressType::Bar(pb) => pb.inc(delta),
            ProgressType::Console => {
                self.count += delta;

                if delta != 0 {
                    println!("{}: Processed {}/{} records", self.model_name, self.count, self.total_rows);
                }
            }
        }
    }

    pub fn finish(self: &mut Self) {
        match &self.progress_type {
            ProgressType::Null => (),
            ProgressType::Bar(pb) => pb.finish(),
            ProgressType::Console => {
                self.count = self.total_rows;
                println!("{}: Processed {}/{} records", self.model_name, self.count, self.total_rows);
            }
        }
    }
}
