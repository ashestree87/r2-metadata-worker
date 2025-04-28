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
            
            // UPDATED APPROACH: Use vision-based OCR as primary method
            // We're prioritizing visual content analysis over text extraction
            console.log(`Using vision-based analysis for PDF ${objectName}`);
            let extractedContent = await useVisionOCR(objectName, base64Data, ai);
            
            // Generate a summary from the extracted content
            console.log(`Extracted content length: ${extractedContent.length} chars`);
            const summaryResult = await generateSummary(objectName, extractedContent, ai);
            
            // Extract tags and clean summary
            const { summary, tags } = processAiResponse(summaryResult);
            
            // 3. Create metadata JSON
            const metadata = {
                filename: objectName,
                type: "pdf",
                summary: summary,
                tags: tags.filter((tag, index) => tags.indexOf(tag) === index), // Remove duplicate tags
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
 * Uses vision-based OCR to analyze PDF content
 * This is now our primary approach for all PDFs
 */
async function useVisionOCR(filename: string, base64Data: string, ai: Ai): Promise<string> {
    try {
        // Create a data URL for the vision model
        const dataUrl = `data:application/pdf;base64,${base64Data}`;
        
        // First page analysis
        const visionPrompt = `
        You are examining a PDF document named "${filename}".
        
        Please analyze this PDF thoroughly and provide a DETAILED description of what you see:
        
        1. What is the overall document about? What's its purpose?
        2. Describe all visible text content including headings, paragraphs, and important text
        3. Describe any visible images, diagrams, charts or graphics
        4. Extract any structured data like tables with their contents
        5. Note any key information like dates, names, prices, or other important facts
        
        Be as specific and comprehensive as possible. Think of yourself as creating a text version
        of this PDF for someone who cannot see it.
        `;
        
        // Use type casting to bypass TypeScript ModelName limitations
        const firstPageResult = await (ai as any).run('@cf/meta/llama-3-8b-vision', {
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
        
        let extractedText = "";
        if (typeof firstPageResult === 'string') {
            extractedText = firstPageResult;
        } else if (firstPageResult && firstPageResult.response) {
            extractedText = firstPageResult.response;
        } else if (firstPageResult && firstPageResult.content) {
            extractedText = firstPageResult.content;
        } else {
            throw new Error("Invalid vision model response format");
        }
        
        // If the PDF might have multiple pages, we should note that in our results
        if (pdfMightHaveMultiplePages(extractedText)) {
            extractedText += "\n\nNote: This PDF may contain multiple pages. The analysis above covers primarily the first visible page.";
        }
        
        return extractedText;
    } catch (error: any) {
        console.error(`Vision-based OCR failed for ${filename}:`, error);
        
        // Try a fallback method with the generic model instead
        try {
            console.log(`Trying fallback extraction for ${filename}`);
            return await fallbackContentExtraction(filename, ai);
        } catch (fallbackError) {
            return `[Content extraction failed: ${error.message || "Unknown error"}. Fallback also failed.]`;
        }
    }
}

/**
 * Check if the PDF might have multiple pages based on the extracted text
 */
function pdfMightHaveMultiplePages(extractedText: string): boolean {
    const multiPageIndicators = [
        /page \d+/i,
        /\bpages?\b/i,
        /continues/i,
        /continued/i,
        /\bnext\b/i,
        /section \d+/i
    ];
    
    return multiPageIndicators.some(pattern => pattern.test(extractedText));
}

/**
 * Fallback method if vision OCR fails
 */
async function fallbackContentExtraction(filename: string, ai: Ai): Promise<string> {
    // Generate content based on the filename as a fallback
    const fallbackPrompt = `
    I have a PDF document named "${filename}" that I cannot process directly.
    Based on this filename, please:
    
    1. Generate a detailed description of what this document likely contains
    2. Make educated guesses about its structure and content
    3. Highlight key elements one might expect to find in such a document
    
    Be specific and practical in your suggestions while acknowledging you haven't seen the actual content.
    `;
    
    const fallbackResult = await ai.run('@cf/meta/llama-3-8b-instruct', {
        messages: [
            { role: 'system', content: 'You are a helpful assistant that can make educated guesses about document content based on filenames.' },
            { role: 'user', content: fallbackPrompt }
        ]
    }) as any;
    
    if (typeof fallbackResult === 'string') {
        return fallbackResult + "\n\n[Note: This is a best-guess description based on the filename, not actual content analysis.]";
    } else if (fallbackResult && fallbackResult.response) {
        return fallbackResult.response + "\n\n[Note: This is a best-guess description based on the filename, not actual content analysis.]";
    }
    
    throw new Error("Fallback extraction failed - invalid response format");
}

/**
 * Generates a summary from extracted PDF content
 */
async function generateSummary(filename: string, extractedContent: string, ai: Ai): Promise<any> {
    // Limit the extracted text to avoid exceeding model context
    const maxLength = 10000;
    const truncatedText = extractedContent.length > maxLength 
        ? extractedContent.substring(0, maxLength) + "... [truncated]" 
        : extractedContent;
    
    const summaryPrompt = `
    I've analyzed the PDF document "${filename}" and extracted the following content:
    
    ${truncatedText}
    
    Based on this content, please:
    
    1. Provide a clear, concise 1-2 paragraph summary of what this document contains and its purpose
    2. Create a list of 5-10 relevant keyword tags that categorize this document content (avoid duplicates)
    
    Format your response as follows:
    [Summary in 1-2 paragraphs]
    
    TAGS: tag1, tag2, tag3, tag4, tag5
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