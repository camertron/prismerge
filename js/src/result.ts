export class OkClass<R, E> {
  constructor(private value: R) {}

  isOk(): this is OkClass<R, E> {
    return true;
  }

  isErr(): this is ErrClass<never, never> {
    return false;
  }

  unwrap(): R {
    return this.value;
  }

  and(callback: (value: R) => void): Result<R, E> {
    callback(this.value);
    return this;
  }

  or(callback: (value: R) => void): Result<R, E> {
    return this;
  }
}

export class ErrClass<R, E> {
  constructor(private _value: E) {}

  isOk(): this is OkClass<never, never> {
    return false;
  }

  isErr(): this is ErrClass<R, E> {
    return true;
  }

  unwrap(): R {
    throw this._value;
  }

  get value(): E {
    return this._value;
  }

  and(callback: (value: R) => void): Result<R, E> {
    return this;
  }

  or(callback: (value: E) => void): Result<R, E> {
    callback(this._value);
    return this;
  }
}

export type Result<R, E> = OkClass<R, E> | ErrClass<R, E>

export function Ok<R, E>(value: R): OkClass<R, E>;
export function Ok<E>(): OkClass<undefined, E>;
export function Ok<R, E>(value?: R): OkClass<R | undefined, E> {
  return new OkClass(value);
}

export function Err<R, E>(value: E): ErrClass<R, E>;
export function Err<R>(): ErrClass<R, undefined>;
export function Err<R, E>(value?: E): ErrClass<R, E | undefined> {
  return new ErrClass(value);
}
