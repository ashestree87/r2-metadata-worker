import { ExecutionContext, R2Object } from '@cloudflare/workers-types';
import { Env } from '../types';
import { Ai } from '@cloudflare/ai';

/**
 * Processes a PDF file from R2:
 * 1. Fetches the PDF object.
 * 2. Uses Cloudflare AI to extract and summarize content.
 * 3. Uploads the metadata back to R2.
 */
export async function processPdf(objectMetadata: R2Object, env: Env, ctx: ExecutionContext): Promise<void> {
    const objectName = objectMetadata.key;
    console.log(`-> Starting PDF processing for ${objectName}`);
    try {
        // 1. Get full PDF object from R2
        const object = await env.MEDIA_BUCKET.get(objectName);
        if (!object) {
            console.error(`Failed to retrieve object ${objectName} from R2.`);
            return; // Skip if object couldn't be retrieved
        }

        // 2. Extract content and summarize using Cloudflare AI
        const pdfBuffer = await object.arrayBuffer();
        
        try {
            // Process the PDF with Cloudflare AI
            const ai = env.AI;
            
            // First, extract text from the PDF - using any type due to incomplete type definitions
            const extractionResult = await ai.run('@cf/baai/bge-large-en-v1.5', {
                document: pdfBuffer
            } as any) as any;
            
            // If we can't extract text, fall back to a placeholder
            if (!extractionResult || !extractionResult.text) {
                throw new Error("Failed to extract text from PDF");
            }
            
            const extractedText = extractionResult.text as string;
            
            // Now generate a summary using the extracted text
            const summaryPrompt = `
            The following is text extracted from a PDF document. 
            Please provide a concise summary (1-2 paragraphs maximum) and 
            extract 5-10 relevant keyword tags:
            
            ${extractedText.slice(0, 5000)}${extractedText.length > 5000 ? '...' : ''}
            `;
            
            const summaryResult = await ai.run('@cf/meta/llama-3-8b-instruct', {
                messages: [
                    { role: 'system', content: 'You are a helpful assistant that summarizes PDF documents and extracts relevant tags.' },
                    { role: 'user', content: summaryPrompt }
                ],
                max_tokens: 500
            }) as any;
            
            // Parse the summary and tags from the AI response
            let summary = summaryResult.response || "Failed to generate summary";
            
            // Extract tags from the summary - look for keywords, lists, or tags sections
            const tagMatches = summary.match(/keywords|tags|key terms|topics|subjects?:?\s*([\w\s,\-]+)/i);
            let tags = ["pdf"];
            
            if (tagMatches && tagMatches[1]) {
                // Extract tags from the matched group
                tags = tagMatches[1]
                    .split(/[,;]/)
                    .map(tag => tag.trim().toLowerCase())
                    .filter(tag => tag.length > 0);
                
                // Remove tags section from summary if found
                summary = summary.replace(/keywords|tags|key terms|topics|subjects?:?\s*([\w\s,\-]+)/i, '').trim();
            }
            
            // 3. Create metadata JSON
            const metadata = {
                filename: objectName,
                type: "pdf",
                summary: summary,
                tags: tags,
                size: object.size,
                lastModified: object.uploaded,
                generatedAt: new Date().toISOString(),
            };

            // 4. Upload metadata back to R2
            const metadataFilename = `${objectName}.metadata.json`;
            await env.MEDIA_BUCKET.put(metadataFilename, JSON.stringify(metadata, null, 2), {
                httpMetadata: { contentType: 'application/json' },
            });

            console.log(`<- Successfully generated metadata for PDF ${objectName}`);
            
        } catch (aiError: any) {
            console.error(`AI processing error for PDF ${objectName}:`, aiError);
            
            // Fall back to basic metadata
            const metadata = {
                filename: objectName,
                type: "pdf",
                summary: "PDF content summarization failed. " + (aiError.message || "Unknown AI processing error."),
                tags: ["pdf", "processing-error"],
                size: object.size,
                lastModified: object.uploaded,
                generatedAt: new Date().toISOString(),
            };

            // Upload basic metadata
            const metadataFilename = `${objectName}.metadata.json`;
            await env.MEDIA_BUCKET.put(metadataFilename, JSON.stringify(metadata, null, 2), {
                httpMetadata: { contentType: 'application/json' },
            });
            
            console.log(`<- Generated basic fallback metadata for PDF ${objectName} due to AI processing error`);
        }

    } catch (error) {
        console.error(`Error processing PDF ${objectName}:`, error);
    }
} 