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
async function processAllMedia(env: Env, ctx: ExecutionContext, options: { forceReprocess?: boolean } = {}): Promise<{ processed: number, skipped: number, errors: number }> {
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
				// Skip metadata files themselves
				if (objectName.endsWith('.metadata.json')) {
					continue;
				}
				
				const metadataFilename = `${objectName}.metadata.json`;

				// 1. Check file extension
				const supportedExtensions = ['.jpg', '.jpeg', '.png', '.mp4', '.pdf'];
				const fileExtension = objectName.substring(objectName.lastIndexOf('.')).toLowerCase();
				if (!supportedExtensions.includes(fileExtension)) {
					// console.log(`Skipping unsupported file type: ${objectName}`);
					stats.skipped++;
					continue; // Skip this object
				}

				// 2. Check if metadata already exists (unless force reprocess is enabled)
				if (!options.forceReprocess) {
					const metadataCheckPromise = env.MEDIA_BUCKET.head(metadataFilename).then(metadataObject => {
						if (metadataObject !== null) {
							// console.log(`Metadata already exists for: ${objectName}`);
							stats.skipped++;
							return; // Metadata exists, skip processing
						}

						return processFile(object, fileExtension, env, ctx, stats);
					}).catch(err => {
						console.error(`Error checking metadata for ${objectName}:`, err);
						stats.errors++;
					});

					processingPromises.push(metadataCheckPromise);
				} else {
					// Force reprocess is enabled, process regardless of existing metadata
					console.log(`Force reprocessing file: ${objectName}`);
					const processPromise = processFile(object, fileExtension, env, ctx, stats)
						.catch(err => {
							console.error(`Error processing ${objectName}:`, err);
							stats.errors++;
						});
					
					processingPromises.push(processPromise);
				}
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

// Helper function to process a single file based on its extension
async function processFile(object: R2Object, fileExtension: string, env: Env, ctx: ExecutionContext, stats: { processed: number, skipped: number, errors: number }): Promise<void> {
	const objectName = object.key;
	console.log(`Processing file: ${objectName}`);
	
	try {
		// Call the appropriate processing function based on fileExtension
		if (['.jpg', '.jpeg', '.png'].includes(fileExtension)) {
			// Enqueue the processing task to ensure it completes
			await processImage(object, env, ctx);
			stats.processed++;
		} else if (fileExtension === '.mp4') {
			// processVideo not yet implemented
			console.log(`Video processing not yet implemented for: ${objectName}`);
			stats.skipped++;
		} else if (fileExtension === '.pdf') {
			// Enqueue the processing task
			await processPdf(object, env, ctx);
			stats.processed++;
		}
	} catch (error) {
		console.error(`Error processing ${objectName}:`, error);
		stats.errors++;
		throw error; // Rethrow to be caught by the caller
	}
}

// HTML template for the UI
function getHtmlTemplate(message = '', processingStats = null, diagnostics = '') {
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
		.diagnostic {
			background-color: #f8fafc;
			padding: 15px;
			border-radius: 8px;
			margin-top: 20px;
			border: 1px solid #e2e8f0;
		}
		.options {
			margin-top: 15px;
		}
		.checkbox-wrapper {
			display: flex;
			align-items: center;
			margin-bottom: 15px;
		}
		.checkbox-wrapper input[type="checkbox"] {
			margin-right: 8px;
		}
	</style>
</head>
<body>
	<h1>R2 Metadata Generator</h1>
	
	<div class="card">
		<h2>Manual Execution</h2>
		<p>Click the button below to start processing media files in your R2 bucket.</p>
		<form method="POST">
			<div class="options">
				<div class="checkbox-wrapper">
					<input type="checkbox" id="force-reprocess" name="force-reprocess" value="1">
					<label for="force-reprocess">Force Reprocess (regenerate metadata even if it already exists)</label>
				</div>
			</div>
			<button type="submit">Process Media Files</button>
		</form>
	</div>

	${diagnostics}

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
	 */
	async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
		console.log(`Scheduled event triggered at: ${new Date(controller.scheduledTime)}`);
		
		// By default, don't force reprocess in scheduled runs
		// To force reprocess, you'd need to trigger it manually with the force option
		const forceReprocess = false;
		
		await processAllMedia(env, ctx, { forceReprocess });
	},

	/**
	 * This function handles HTTP requests to enable manual execution and provide a simple UI.
	 */
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		
		// Check if this is a cron trigger or similar service worker by looking at the user agent
		const userAgent = request.headers.get('User-Agent') || '';
		const isCronTrigger = userAgent.includes('Cloudflare-Workers') || 
							userAgent.includes('Cloudflare-Scheduler') || 
							userAgent.includes('Cronjob');
							
		if (isCronTrigger) {
			console.log('Detected cron trigger or service worker request, running scheduled processing');
			
			// Check for force parameter in scheduled requests too
			const forceReprocess = url.searchParams.has('force');
			
			// Process media but return minimal response
			try {
				const stats = await processAllMedia(env, ctx, { forceReprocess });
				return new Response(JSON.stringify({
					status: 'success',
					message: 'Scheduled processing completed',
					forceReprocess,
					stats
				}), {
					headers: { 'Content-Type': 'application/json' }
				});
			} catch (error: any) {
				return new Response(JSON.stringify({
					status: 'error',
					message: error.message || 'Unknown error'
				}), {
					status: 500,
					headers: { 'Content-Type': 'application/json' }
				});
			}
		}
		
		// Add a simple test endpoint to debug R2 access
		if (url.pathname === '/test-r2') {
			// Simple logging to debug
			console.log('MEDIA_BUCKET binding type:', typeof env.MEDIA_BUCKET);
			console.log('Available environment bindings:', Object.keys(env));
			
			try {
				if (!env.MEDIA_BUCKET) {
					return new Response(JSON.stringify({
						success: false,
						error: 'MEDIA_BUCKET binding is undefined',
						availableBindings: Object.keys(env),
						environment: {
							// Include additional environment info
							nodeVersion: process.versions?.node || 'unknown',
							bindingType: typeof env.MEDIA_BUCKET,
							hasMediaBucketProperty: 'MEDIA_BUCKET' in env
						}
					}, null, 2), { 
						status: 500,
						headers: { 'Content-Type': 'application/json' }
					});
				}
				
				// Check if the binding has the correct type
				if (typeof env.MEDIA_BUCKET.list !== 'function') {
					return new Response(JSON.stringify({
						success: false,
						error: 'MEDIA_BUCKET binding exists but does not appear to be an R2 bucket',
						bindingType: typeof env.MEDIA_BUCKET,
						hasListMethod: typeof env.MEDIA_BUCKET.list === 'function'
					}, null, 2), { 
						status: 500,
						headers: { 'Content-Type': 'application/json' }
					});
				}
				
				// Try listing just 1 object to test access
				const listing = await env.MEDIA_BUCKET.list({
					limit: 1
				});
				
				return new Response(JSON.stringify({
					success: true,
					bucketAvailable: true,
					objectCount: listing.objects.length,
					objects: listing.objects.map(obj => ({
						key: obj.key,
						size: obj.size
					}))
				}, null, 2), {
					headers: { 'Content-Type': 'application/json' }
				});
			} catch (error: any) {
				return new Response(JSON.stringify({
					success: false,
					error: error.message,
					stack: error.stack,
					bucketAvailable: !!env.MEDIA_BUCKET
				}, null, 2), {
					status: 500,
					headers: { 'Content-Type': 'application/json' }
				});
			}
		}
		
		// Generate diagnostics HTML
		const diagnosticsHtml = `
		<div class="diagnostic">
			<h3>Binding Diagnostics</h3>
			<p>R2 Bucket Binding Status: <strong>${typeof env.MEDIA_BUCKET === 'undefined' ? '❌ Missing' : '✅ Available'}</strong></p>
			<p>AI Binding Status: <strong>${typeof env.AI === 'undefined' ? '❌ Missing' : '✅ Available'}</strong></p>
			<p><a href="/test-r2" target="_blank">Run R2 Connection Test</a></p>
		</div>`;
		
		// Handle POST request (manual execution) - ONLY process on POST
		if (request.method === 'POST') {
			try {
				// Check if forceReprocess is enabled
				let formData: FormData | null = null;
				let forceReprocess = false;
				
				try {
					formData = await request.formData();
					forceReprocess = formData.has('force-reprocess');
				} catch (e) {
					// If we can't parse form data, proceed without force reprocess
					console.log('Could not parse form data:', e);
				}
				
				console.log(`Processing with force reprocess: ${forceReprocess}`);
				
				const stats = await processAllMedia(env, ctx, { forceReprocess });
				return new Response(getHtmlTemplate(`Successfully executed media processing at ${new Date().toISOString()}`, stats, diagnosticsHtml), {
					headers: { 'Content-Type': 'text/html' },
				});
			} catch (error: any) {
				console.error('Error during manual execution:', error);
				return new Response(getHtmlTemplate(`Error: ${error.message || 'Unknown error during processing'}`, null, diagnosticsHtml), {
					headers: { 'Content-Type': 'text/html' },
					status: 500,
				});
			}
		}
		
		// Default: ONLY show UI for GET requests, no processing
		return new Response(getHtmlTemplate('', null, diagnosticsHtml), {
			headers: { 'Content-Type': 'text/html' },
		});
	},
};
