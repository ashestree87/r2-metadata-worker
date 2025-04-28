import { ExecutionContext, R2Object } from '@cloudflare/workers-types';
import { Env } from '../types';

// Helper function to convert Blob to Base64
async function blobToBase64(blob: Blob): Promise<string> {
    const arrayBuffer = await blob.arrayBuffer();
    let binary = '';
    const bytes = new Uint8Array(arrayBuffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}

/**
 * Processes an image file from R2:
 * 1. Fetches the image blob.
 * 2. Calls OpenAI Vision API (gpt-4o) to get caption and tags.
 * 3. Creates a JSON metadata object.
 * 4. Uploads the metadata back to R2.
 */
export async function processImage(objectMetadata: R2Object, env: Env, ctx: ExecutionContext): Promise<void> {
	const objectName = objectMetadata.key;
	console.log(`-> Starting image processing for ${objectName}`);
	try {
		// 1. Get full image object from R2
		const object = await env.MEDIA_BUCKET.get(objectName);
		if (!object) {
			console.error(`Failed to retrieve object ${objectName} from R2.`);
			return; // Skip if object couldn't be retrieved
		}
		const imageBlob = await object.blob();
        const imageMimeType = object.httpMetadata?.contentType || 'image/jpeg'; // Default or get from metadata

		// 2. Call OpenAI Vision API
        const base64Image = await blobToBase64(imageBlob);
        const openAiPayload = {
            model: "gpt-4o",
            messages: [
                {
                    role: "user",
                    content: [
                        {
                            type: "text",
                            text: "Describe this image in 1-2 concise sentences. Also, provide a short list of relevant keywords (tags) as a JSON array. Respond ONLY with a JSON object containing 'caption' and 'tags' keys. Example: { \"caption\": \"A sunny beach with palm trees.\", \"tags\": [\"beach\", \"sunny\", \"palm trees\"] }"
                        },
                        {
                            type: "image_url",
                            image_url: {
                                url: `data:${imageMimeType};base64,${base64Image}`
                            }
                        }
                    ]
                }
            ],
            max_tokens: 300,
            // Ensure response is JSON
            response_format: { type: "json_object" }, 
        };

        const openAiResponse = await fetch("https://api.openai.com/v1/chat/completions", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${env.OPENAI_API_KEY}`
            },
            body: JSON.stringify(openAiPayload)
        });

        if (!openAiResponse.ok) {
            const errorText = await openAiResponse.text();
            throw new Error(`OpenAI API Error: ${openAiResponse.status} ${openAiResponse.statusText} - ${errorText}`);
        }

        const openAiResult = await openAiResponse.json() as any; // Type assertion for simplicity
        const assistantResponse = openAiResult.choices?.[0]?.message?.content;

        if (!assistantResponse) {
             throw new Error('Invalid response structure from OpenAI API');
        }

        let caption = "Error parsing caption";
        let tags: string[] = ["error"];

        try {
            const parsedContent = JSON.parse(assistantResponse);
            caption = parsedContent.caption || "Caption not found in response";
            tags = parsedContent.tags || ["Tags not found in response"];
            if (!Array.isArray(tags)) tags = ["Invalid tags format"]; // Ensure tags is an array
        } catch (parseError) {
            console.error("Error parsing OpenAI JSON response:", parseError, "Raw response:", assistantResponse);
             throw new Error('Failed to parse JSON response from OpenAI API');
        }

		// 3. Create metadata JSON
		const metadata = {
			filename: objectName,
			type: "image",
			caption: caption,
			tags: tags,
			size: object.size, // Use size from the retrieved object body
			lastModified: object.uploaded, // Use uploaded date from the retrieved object body
			generatedAt: new Date().toISOString(), // Add timestamp of metadata generation
		};

		// 4. Upload metadata back to R2
		const metadataFilename = `${objectName}.metadata.json`;
		await env.MEDIA_BUCKET.put(metadataFilename, JSON.stringify(metadata, null, 2), {
			httpMetadata: { contentType: 'application/json' },
		});

		console.log(`<- Successfully generated and uploaded metadata for ${objectName}`);

	} catch (error) {
		console.error(`Error processing image ${objectName}:`, error);
		// Handle errors gracefully, maybe add retry logic or log failures
	}
} 