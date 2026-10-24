export interface EmberOptions {
  baseUrl: string;
  apiKey: string;
  /** Retries for network errors and 503 (overloaded). Default 5. */
  maxRetries?: number;
  /** Per-request timeout in ms. Default 60000. */
  timeoutMs?: number;
  fetch?: typeof fetch;
}

export class EmberError extends Error {
  status: number;
  body: any;
}

export interface PublishResult { topic?: string; partition?: number; offset?: number; error?: string }

export class Message {
  partition: number;
  offset: number;
  /** 1 on the first delivery, then 2, 3... */
  attempt: number;
  timestamp: number;
  key: string | null;
  /** UTF-8 text, or "base64:..." when the bytes are not valid UTF-8. */
  value: string;
  json<T = any>(): T;
  readonly handle: { partition: number; offset: number; attempt: number };
}

export interface QueueInfo {
  queue: string; topic: string; leaseMs: number; maxAttempts: number; backoffBaseMs: number; backoffMaxMs: number;
  waiting: number; inFlight: number; retrying: number; dead: number;
  partitions: { partition: number; floor: number; next: number; logEnd: number; inFlight: number; retrying: number; dead: number }[];
}

export interface WorkerOptions {
  /** Worker name, unique per running worker of your app. Default "default". */
  name?: string;
  /** Messages handled at the same time. Default 10. */
  concurrency?: number;
  /** Most messages claimed per request. Default = concurrency (max 500). */
  batchSize?: number;
  /** Long-poll time when the queue is empty. Default 20000. */
  waitMs?: number;
  /** Lease for each claim in ms (0 = the queue's default). */
  leaseMs?: number;
  /** How often running messages get their lease extended. Default 10000. */
  extendEveryMs?: number;
  /** How long stop() waits for running handlers. Default 30000. */
  drainMs?: number;
  /** Acks are grouped for this many ms. Default 5. */
  ackDelayMs?: number;
  /**
   * "key": messages with the same key are handled one at a time in the order this worker received them, while different keys still run
   * in parallel (up to `concurrency`). Only within ONE worker: another worker, or a redelivery after a failure, can still run a key's
   * messages out of order. A hot key holds the slots of its queued messages. Default "none".
   */
  order?: 'none' | 'key';
  /**
   * Only the newest message (highest offset) of each key is handled; older ones, in the same batch or arriving after a newer one was
   * handled by this worker, are acknowledged without calling the handler (counted in stats.superseded). For state where the last
   * value is all that matters (positions, prices, statuses). Messages without a key are always handled.
   */
  latestWins?: boolean;
  /** How many keys' newest handled offset latestWins remembers (least recently used are forgotten). Default 100000. */
  latestWinsKeys?: number;
  onError?: (error: Error, context: { phase: string; message?: Message }) => void;
}

export class Worker {
  stats: { claimed: number; acked: number; nacked: number; released: number; rejected: number; superseded: number };
  start(): this;
  stop(): Promise<void>;
  /** Chaos helper: act like a dead process (settle nothing; claimed messages come back when their lease runs out). */
  crash(): void;
}

export const RELEASE: unique symbol;

export class Ember {
  constructor(options: EmberOptions);
  static RELEASE: typeof RELEASE;
  publish(topic: string, message: { key?: string | number | null; value: unknown }): Promise<PublishResult>;
  publishBatch(topic: string, messages: { key?: string | number | null; value: unknown }[]): Promise<PublishResult[]>;
  queue(name: string): Promise<QueueInfo>;
  claim(queue: string, options?: { max?: number; leaseMs?: number; waitMs?: number; worker?: string; signal?: AbortSignal }): Promise<Message[]>;
  ack(queue: string, messages: Message[], worker?: string): Promise<{ settled: number; rejected: number }>;
  nack(queue: string, messages: Message[], reason: string, worker?: string): Promise<{ settled: number; rejected: number }>;
  release(queue: string, messages: Message[], worker?: string): Promise<{ settled: number; rejected: number }>;
  extend(queue: string, messages: Message[], leaseMs?: number, worker?: string): Promise<{ settled: number; rejected: number }>;
  /** Handler resolves: ack. Throws: nack (retry with backoff, then dead letter). Returns Ember.RELEASE: give back without a failure. */
  worker(queue: string, handler: (message: Message) => unknown | Promise<unknown>, options?: WorkerOptions): Worker;
}
