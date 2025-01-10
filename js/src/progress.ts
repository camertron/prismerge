import cliProgress from "cli-progress";

/* A wrapper around the cli-progress module that falls back to regular 'ol console logging
 * if STDIN isn't a terminal. Progress bars don't work super well in GitHub Actions and
 * end up writing nothing, making it difficult to track merge progress.
 */
export abstract class ProgressIndicator {
  static create(model_name: string, total_rows: number): ProgressIndicator {
    if (process.stdin.isTTY) {
      return new ProgressBar(model_name, total_rows);
    } else {
      return new ConsoleProgress(model_name, total_rows);
    }
  }

  static null() {
    return NullProgress.instance();
  }

  abstract inc(delta: number): void;
  abstract finish(): void;
}

class ConsoleProgress implements ProgressIndicator {
  private count: number;

  constructor(
    private model_name: string,
    private total_rows: number
  ) {
    this.count = 0;
  }

  inc(delta: number): void {
    this.count += delta;

    if (delta != 0) {
      console.log(`${this.model_name}: Processed ${this.count}/${this.total_rows} records`);
    }
  }

  finish(): void {
    this.count = this.total_rows;
    console.log(`${this.model_name}: Processed ${this.count}/${this.total_rows} records`);
  }
}

class ProgressBar implements ProgressIndicator {
  private bar: cliProgress.Bar;

  constructor(private model_name: string, private total_rows: number) {
    const options: cliProgress.Options = {
      format: `${this.model_name} [{duration_formatted}] {bar} {value}/{total}`,
      autopadding: true
    };

    this.bar = new cliProgress.SingleBar(options, cliProgress.Presets.legacy);
    this.bar.start(this.total_rows, 0);
  }

  inc(delta: number): void {
    this.bar.increment(delta);
  }

  finish(): void {
    this.bar.update(this.total_rows);
    this.bar.stop();
  }
}

class NullProgress implements ProgressIndicator {
  static _instance: NullProgress;

  static instance(): NullProgress {
    if (!this._instance) {
      this._instance = new NullProgress();
    }

    return this._instance;
  }

  inc(delta: number): void {
  }

  finish(): void {
  }
}