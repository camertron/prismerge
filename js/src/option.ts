export class SomeClass<T> {
  private value: T;

  constructor(value: T) {
    this.value = value;
  }

  isSome(): this is SomeClass<T> {
    return true;
  }

  unwrap(): T {
    return this.value;
  }

  and(callback: (value: T) => void): Option<T> {
    callback(this.value);
    return this;
  }

  or(callback: () => void): Option<T> {
    return this;
  }
}

export const Some = <T>(value: T): SomeClass<T> => {
  return new SomeClass(value);
}

export class NoneClass<T> {
  isSome(): this is SomeClass<never> {
    return false;
  }

  unwrap(): T {
    throw new Error("Tried to unwrap a None");
  }

  and(callback: (value: T) => void): Option<T> {
    return this;
  }

  or(callback: () => void): Option<T> {
    callback();
    return this;
  }
}

export const None = <T>(): NoneClass<T> => {
  return new NoneClass();
};

export type Option<T> = SomeClass<T> | NoneClass<T>;
