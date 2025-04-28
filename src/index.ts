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

export default {
	/**
	 * This function is triggered by the cron schedule defined in wrangler.toml.
	 * @param controller - Contains metadata about the scheduled event.
	 * @param env - Contains the bindings configured in wrangler.toml.
	 * @param ctx - Execution context, used for tasks like waiting for promises to settle.
	 */
	async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
		console.log(`Scheduled event triggered at: ${new Date(controller.scheduledTime)}`);

		// TODO: Implement R2 bucket scanning logic here
		console.log('Accessing R2 bucket:', env.MEDIA_BUCKET);

		// Example: List objects in the bucket (we will refine this)
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
						continue; // Skip this object
					}

					// 2. Check if metadata already exists
					// We can do a quick HEAD request to see if the metadata file exists.
					// Note: This adds an extra R2 operation per file.
					const metadataCheckPromise = env.MEDIA_BUCKET.head(metadataFilename).then(metadataObject => {
						if (metadataObject !== null) {
							// console.log(`Metadata already exists for: ${objectName}`);
							return; // Metadata exists, skip processing
						}

						// Metadata doesn't exist, proceed with processing
						console.log(`Processing file: ${objectName}`);
						// Call the appropriate processing function based on fileExtension
						if (['.jpg', '.jpeg', '.png'].includes(fileExtension)) {
							// Enqueue the processing task to ensure it completes
							ctx.waitUntil(processImage(object, env, ctx));
						} else if (fileExtension === '.mp4') {
							// ctx.waitUntil(processVideo(object, env, ctx));
						} else if (fileExtension === '.pdf') {
							// Enqueue the processing task
							ctx.waitUntil(processPdf(object, env, ctx));
						}
					}).catch(err => {
						console.error(`Error checking metadata for ${objectName}:`, err);
						// Decide if you want to continue processing or skip on error
					});

					processingPromises.push(metadataCheckPromise);
				}

				// Wait for all metadata checks/processing initiations in this batch to potentially start
				// Note: The actual processing logic will likely be async and needs careful handling with ctx.waitUntil
				// We remove the waitUntil here as the individual process calls handle it.

				truncated = listing.truncated;
				cursor = listing.truncated ? listing.cursor : undefined;
			}

			console.log('Finished listing objects.');

		} catch (error) {
			console.error('Error listing R2 bucket:', error);
			// Consider adding more robust error handling/reporting here
		}
		
		// Ensure all asynchronous tasks complete before the worker execution ends
		// ctx.waitUntil(/* Promise or array of Promises */);
	},
};
