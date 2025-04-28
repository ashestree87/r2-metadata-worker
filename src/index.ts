/**
 * Welcome to Cloudflare Workers!
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bindings are configured in `wrangler.toml`. When running locally,
 * `wrangler dev` uses the `[vars]` configuration in `wrangler.toml`
 * to populate bindings.
 *
 * Learn more about developing Workers at https://developers.cloudflare.com/workers/
 */

import { ExecutionContext, R2Bucket, ScheduledController } from '@cloudflare/workers-types';
import { Env } from './types'; // Import Env from the new file
import { processImage } from './processors/image'; // Import processImage
import { processPdf } from './processors/pdf'; // Import processPdf

// Define the environment bindings expected by the worker - MOVED to types.ts
// export interface Env { ... }

// Helper function to convert Blob to Base64 - MOVED to processors/image.ts
// async function blobToBase64(blob: Blob): Promise<string> { ... }

// Function to process images - MOVED to processors/image.ts
// async function processImage(objectMetadata: R2Object, env: Env, ctx: ExecutionContext): Promise<void> { ... }

// Function to process PDF files - MOVED to processors/pdf.ts
// async function processPdf(objectMetadata: R2Object, env: Env, ctx: ExecutionContext): Promise<void> { ... }

// TODO: Define processVideo function similarly

// Helper function to contain our main worker logic
async function processAllMedia(env: Env, ctx: ExecutionContext, options = {}): Promise<{ processed: number, skipped: number, errors: number }> {
	console.log('Starting media processing...');
	
	const stats = {
		processed: 0,
		skipped: 0,
		errors: 0
	};

	try {
		const listOptions = {
			prefix: '', // List all objects
			limit: 500, // Adjust as needed, max 1000
			delimiter: undefined, // Do not group by directories
			include: ['httpMetadata', 'customMetadata'], // Include metadata if needed
		};

		let truncated = true;
		let cursor: string | undefined = undefined;

		while (truncated) {
			const listing = await env.MEDIA_BUCKET.list({
				...listOptions,
				cursor: cursor,
			});

			console.log(`Found ${listing.objects.length} objects in this batch.`);

			// Process the objects found in listing.objects
			const processingPromises: Promise<void>[] = [];
			for (const object of listing.objects) {
				const objectName = object.key;
				const metadataFilename = `${objectName}.metadata.json`;

				// 1. Check file extension
				const supportedExtensions = ['.jpg', '.jpeg', '.png', '.mp4', '.pdf'];
				const fileExtension = objectName.substring(objectName.lastIndexOf('.')).toLowerCase();
				if (!supportedExtensions.includes(fileExtension)) {
					// console.log(`Skipping unsupported file type: ${objectName}`);
					stats.skipped++;
					continue; // Skip this object
				}

				// 2. Check if metadata already exists
				// We can do a quick HEAD request to see if the metadata file exists.
				// Note: This adds an extra R2 operation per file.
				const metadataCheckPromise = env.MEDIA_BUCKET.head(metadataFilename).then(metadataObject => {
					if (metadataObject !== null) {
						// console.log(`Metadata already exists for: ${objectName}`);
						stats.skipped++;
						return; // Metadata exists, skip processing
					}

					// Metadata doesn't exist, proceed with processing
					console.log(`Processing file: ${objectName}`);
					// Call the appropriate processing function based on fileExtension
					if (['.jpg', '.jpeg', '.png'].includes(fileExtension)) {
						// Enqueue the processing task to ensure it completes
						try {
							ctx.waitUntil(processImage(object, env, ctx));
							stats.processed++;
						} catch (error) {
							console.error(`Error processing image ${objectName}:`, error);
							stats.errors++;
						}
					} else if (fileExtension === '.mp4') {
						// ctx.waitUntil(processVideo(object, env, ctx));
						console.log(`Video processing not yet implemented for: ${objectName}`);
						stats.skipped++;
					} else if (fileExtension === '.pdf') {
						// Enqueue the processing task
						try {
							ctx.waitUntil(processPdf(object, env, ctx));
							stats.processed++;
						} catch (error) {
							console.error(`Error processing PDF ${objectName}:`, error);
							stats.errors++;
						}
					}
				}).catch(err => {
					console.error(`Error checking metadata for ${objectName}:`, err);
					stats.errors++;
				});

				processingPromises.push(metadataCheckPromise);
			}

			// Wait for all metadata checks to resolve
			await Promise.allSettled(processingPromises);

			truncated = listing.truncated;
			cursor = listing.truncated ? listing.cursor : undefined;
		}

		console.log('Finished listing objects.');

	} catch (error) {
		console.error('Error listing R2 bucket:', error);
		stats.errors++;
	}
	
	return stats;
}

// HTML template for the UI
function getHtmlTemplate(message = '', processingStats = null) {
	return `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>R2 Metadata Generator</title>
	<style>
		body {
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
			line-height: 1.6;
			color: #333;
			max-width: 800px;
			margin: 0 auto;
			padding: 20px;
		}
		h1 {
			color: #2563eb;
		}
		.card {
			background-color: #f9fafb;
			border-radius: 8px;
			padding: 20px;
			margin-bottom: 20px;
			box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
		}
		button {
			background-color: #2563eb;
			color: white;
			border: none;
			border-radius: 4px;
			padding: 10px 15px;
			cursor: pointer;
			font-size: 16px;
		}
		button:hover {
			background-color: #1d4ed8;
		}
		pre {
			background-color: #f1f5f9;
			padding: 15px;
			border-radius: 4px;
			overflow-x: auto;
		}
		.status {
			margin-top: 20px;
			padding: 15px;
			border-radius: 4px;
		}
		.status.success {
			background-color: #ecfdf5;
			border-left: 4px solid #10b981;
		}
		.status.error {
			background-color: #fef2f2;
			border-left: 4px solid #ef4444;
		}
		.stats {
			display: grid;
			grid-template-columns: repeat(3, 1fr);
			gap: 10px;
			margin-top: 20px;
		}
		.stat-card {
			background-color: #ffffff;
			border-radius: 8px;
			padding: 15px;
			box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
			text-align: center;
		}
		.stat-card h3 {
			margin-top: 0;
			font-size: 14px;
			color: #6b7280;
		}
		.stat-card p {
			margin-bottom: 0;
			font-size: 24px;
			font-weight: bold;
			color: #2563eb;
		}
		.stat-card.errors p {
			color: #ef4444;
		}
	</style>
</head>
<body>
	<h1>R2 Metadata Generator</h1>
	
	<div class="card">
		<h2>Manual Execution</h2>
		<p>Click the button below to start processing media files in your R2 bucket.</p>
		<form method="POST">
			<button type="submit">Process Media Files</button>
		</form>
	</div>

	${message ? `
	<div class="status ${message.includes('Error') ? 'error' : 'success'}">
		${message}
	</div>
	` : ''}

	${processingStats ? `
	<div class="stats">
		<div class="stat-card">
			<h3>Processed</h3>
			<p>${processingStats.processed}</p>
		</div>
		<div class="stat-card">
			<h3>Skipped</h3>
			<p>${processingStats.skipped}</p>
		</div>
		<div class="stat-card errors">
			<h3>Errors</h3>
			<p>${processingStats.errors}</p>
		</div>
	</div>
	` : ''}

	<div class="card">
		<h2>How It Works</h2>
		<p>This worker scans your R2 bucket for media files and generates descriptive metadata:</p>
		<ul>
			<li><strong>Images:</strong> Generates captions and tags using OpenAI's vision models</li>
			<li><strong>PDFs:</strong> Creates basic metadata (currently placeholder for summarization)</li>
			<li><strong>Videos:</strong> Not yet implemented</li>
		</ul>
		<p>Metadata is stored alongside the original files as JSON.</p>
	</div>
</body>
</html>`;
}

export default {
	/**
	 * This function is triggered by the cron schedule defined in wrangler.toml.
	 * @param controller - Contains metadata about the scheduled event.
	 * @param env - Contains the bindings configured in wrangler.toml.
	 * @param ctx - Execution context, used for tasks like waiting for promises to settle.
	 */
	async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
		console.log(`Scheduled event triggered at: ${new Date(controller.scheduledTime)}`);
		await processAllMedia(env, ctx);
	},

	/**
	 * This function handles HTTP requests to enable manual execution and provide a simple UI.
	 */
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		
		// Handle POST request (manual execution)
		if (request.method === 'POST') {
			try {
				const stats = await processAllMedia(env, ctx);
				return new Response(getHtmlTemplate(`Successfully executed media processing at ${new Date().toISOString()}`, stats), {
					headers: { 'Content-Type': 'text/html' },
				});
			} catch (error: any) {
				console.error('Error during manual execution:', error);
				return new Response(getHtmlTemplate(`Error: ${error.message || 'Unknown error during processing'}`), {
					headers: { 'Content-Type': 'text/html' },
					status: 500,
				});
			}
		}
		
		// Default: show UI for GET requests
		return new Response(getHtmlTemplate(), {
			headers: { 'Content-Type': 'text/html' },
		});
	},
};
