import { DateTime, Str } from "chanfana";
import type { Context } from "hono";
import { z } from "zod";
import { R2Bucket } from '@cloudflare/workers-types';

export type AppContext = Context<{ Bindings: Env }>;

export const Task = z.object({
	name: Str({ example: "lorem" }),
	slug: Str(),
	description: Str({ required: false }),
	completed: z.boolean().default(false),
	due_date: DateTime(),
});

/**
 * Defines the environment bindings expected by the worker.
 * These must match the bindings configured in wrangler.toml
 */
export interface Env {
	// Binding for the R2 bucket
	MEDIA_BUCKET: R2Bucket;

	// Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
	// MY_KV_NAMESPACE: KVNamespace;
	//
	// Example binding to Durable Object. Learn more at https://developers.cloudflare.com/workers/runtime-apis/durable-objects/
	// MY_DURABLE_OBJECT: DurableObjectNamespace;
	//
	// Example binding to a Service. Learn more at https://developers.cloudflare.com/workers/runtime-apis/service-bindings/
	// MY_SERVICE: Fetcher;
	//
	// Example binding to a Queue. Learn more at https://developers.cloudflare.com/queues/javascript-apis/
	// MY_QUEUE: Queue;

	// Secrets (like API keys) should be stored using Wrangler secrets
	// Learn more at https://developers.cloudflare.com/workers/wrangler/commands/#secret
	OPENAI_API_KEY: string;
}
