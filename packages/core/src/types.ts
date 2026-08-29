export type JsonPrimitive = string | number | boolean | null;
export type JsonArray = JsonValue[];
export interface JsonObject {
  [key: string]: JsonValue;
}
export type JsonValue = JsonPrimitive | JsonObject | JsonArray;

export type JobStatus = 'pending' | 'active' | 'completed' | 'dead';

export interface Job<T extends JsonValue = JsonObject> {
  id: string;
  queue_name: string;
  payload: T;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  priority: number;
  run_at: Date;
  locked_at: Date | null;
  locked_by: string | null;
  lease_expires_at: Date | null;
  last_error: string | null;
  created_at: Date;
  updated_at: Date;
}

export interface EnqueueOptions<T extends JsonValue = JsonObject> {
  queueName: string;
  payload: T;
  priority?: number;
  maxAttempts?: number;
  runAt?: Date;
}