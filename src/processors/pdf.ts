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
            const ai = env.AI;
            
            // Convert the PDF to base64 (ensuring we respect size limits)
            const maxBytes = 1024 * 1024; // 1MB max for input
            const limitedBuffer = pdfBuffer.slice(0, Math.min(pdfBuffer.byteLength, maxBytes));
            const bytes = new Uint8Array(limitedBuffer);
            
            // Convert binary data to string
            let binaryString = '';
            for (let i = 0; i < bytes.byteLength; i++) {
                binaryString += String.fromCharCode(bytes[i]);
            }
            
            // Convert to base64
            const base64Data = btoa(binaryString);
            
            console.log(`Converted PDF to base64, size: ${base64Data.length} chars`);
            
            // Approach 1: Try text extraction first
            let extractedText = await tryTextExtraction(objectName, bytes, ai);

            // Approach 2: If text extraction didn't yield meaningful results, try vision-based approach
            if (!extractedText || extractedText.trim().length < 100) {
                console.log(`Text extraction yielded minimal results, trying vision approach for PDF ${objectName}`);
                extractedText = await tryVisionExtraction(objectName, base64Data, ai);
            }
            
            // Generate a summary from the extracted content
            console.log(`Extracted content length: ${extractedText.length} chars`);
            const summaryResult = await generateSummary(objectName, extractedText, ai);
            
            // Extract tags and clean summary
            const { summary, tags } = processAiResponse(summaryResult);
            
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
            
            // Fall back to basic metadata with filename-based guessing
            const filenameWords = objectName
                .replace(/\.pdf$/i, '')
                .split(/[_\-\s.]+/)
                .filter(word => word.length > 2);
                
            const metadata = {
                filename: objectName,
                type: "pdf",
                summary: `PDF processing error: ${aiError.message || "Unknown AI error"}`,
                tags: ["pdf", "processing-error", ...filenameWords.slice(0, 3)],
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

/**
 * Attempts to extract text content from a PDF using an LLM approach
 */
async function tryTextExtraction(filename: string, bytes: Uint8Array, ai: Ai): Promise<string> {
    // First, try PDF text extraction with Llama model
    const extractionPrompt = `
    You are an expert PDF text extractor. I'll provide the beginning bytes of a PDF document. 
    Based on your knowledge of PDF structure:
    
    1. Identify any text content in this file
    2. Especially focus on extracting titles, headings, and key content sections
    3. Extract any table content if present
    
    PDF Name: "${filename}"
    First few bytes (hex): ${Array.from(bytes.slice(0, 100)).map(b => b.toString(16).padStart(2, '0')).join(' ')}
    `;
    
    // Try to extract text and metadata from the PDF
    const extractionResult = await ai.run('@cf/meta/llama-3-8b-instruct', {
        messages: [
            { role: 'system', content: 'You are an expert at extracting and understanding PDF document contents.' },
            { role: 'user', content: extractionPrompt }
        ]
    }) as any;
    
    if (typeof extractionResult === 'string') {
        return extractionResult;
    } else if (extractionResult && extractionResult.response) {
        return extractionResult.response;
    }
    
    return ""; // Return empty string if extraction failed
}

/**
 * Attempts to extract content from a PDF using a vision-based approach
 * for image-based or scanned PDFs
 */
async function tryVisionExtraction(filename: string, base64Data: string, ai: Ai): Promise<string> {
    try {
        // For image-based PDFs, we'll treat the PDF as an image and use vision models
        // We need to convert our base64 data to a data URL for the vision model
        const dataUrl = `data:application/pdf;base64,${base64Data}`;
        
        const visionPrompt = `
        This is a PDF document named "${filename}". 
        Please analyze this as if it's a scanned document or image-based PDF.
        
        1. What text content can you see in this PDF?
        2. Describe any visible titles, headings, paragraphs and images
        3. Extract any table data if visible
        4. Look for any important information like dates, names, or key facts
        
        Please provide a detailed description of all visible content.
        `;
        
        // Use the vision model to "see" what's in the PDF
        // Use type casting to bypass TypeScript ModelName limitations
        const visionResult = await (ai as any).run('@cf/meta/llama-3-8b-vision', {
            messages: [
                { 
                    role: 'user', 
                    content: [
                        { type: 'text', text: visionPrompt },
                        { type: 'image_url', image_url: { url: dataUrl } }
                    ]
                }
            ]
        });
        
        if (typeof visionResult === 'string') {
            return visionResult;
        } else if (visionResult && visionResult.response) {
            return visionResult.response;
        } else if (visionResult && visionResult.content) {
            return visionResult.content;
        }
        
        throw new Error("Invalid vision model response format");
    } catch (error: any) {
        console.error(`Vision-based extraction failed for ${filename}:`, error);
        return `[Vision extraction failed: ${error.message || "Unknown error"}]`;
    }
}

/**
 * Generates a summary from extracted PDF content
 */
async function generateSummary(filename: string, extractedText: string, ai: Ai): Promise<any> {
    // Limit the extracted text to avoid exceeding model context
    const maxLength = 10000;
    const truncatedText = extractedText.length > maxLength 
        ? extractedText.substring(0, maxLength) + "... [truncated]" 
        : extractedText;
    
    const summaryPrompt = `
    I've extracted the following content from a PDF document named "${filename}":
    
    ${truncatedText}
    
    Based on this content, please:
    
    1. Provide a concise 1-2 paragraph summary of what this document contains
    2. Create a list of 5-10 relevant keyword tags that categorize this document content
    
    Format your response with a summary paragraph followed by "TAGS:" and then a comma-separated list of tags.
    `;
    
    return ai.run('@cf/meta/llama-3-8b-instruct', {
        messages: [
            { role: 'system', content: 'You are a helpful assistant that summarizes documents and extracts relevant tags.' },
            { role: 'user', content: summaryPrompt }
        ]
    });
}

/**
 * Processes the AI response to extract summary and tags
 */
function processAiResponse(summaryResult: any): { summary: string, tags: string[] } {
    // Parse the summary and tags from the AI response
    let summary = "";
    if (typeof summaryResult === 'string') {
        summary = summaryResult;
    } else if (summaryResult && summaryResult.response) {
        summary = summaryResult.response;
    } else {
        throw new Error("Failed to generate summary - invalid response format");
    }
    
    // Extract tags from the summary - look for keywords, lists, or tags sections
    let tags = ["pdf"];
    const tagMatch = summary.match(/tags:(.+?)(?:\n\n|\n$|$)/i);
    
    if (tagMatch && tagMatch[1]) {
        // Extract tags from the matched group
        const tagText = tagMatch[1].trim();
        tags = tagText
            .split(/[,;]/)
            .map(tag => tag.trim().toLowerCase())
            .filter(tag => tag.length > 0);
        
        if (tags.length === 0) {
            tags = ["pdf"]; // Fallback if parsing tags failed
        }
        
        // Remove tags section from summary if found
        summary = summary.replace(/tags:(.+?)(?:\n\n|\n$|$)/i, '').trim();
    }
    
    return { summary, tags };
} 