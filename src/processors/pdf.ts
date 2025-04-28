import { ExecutionContext, R2Object } from '@cloudflare/workers-types';
import { Env } from '../types';
import { Ai } from '@cloudflare/ai';

/**
 * Processes a PDF file from R2:
 * 1. Fetches the PDF object.
 * 2. Uses Cloudflare AI vision model to analyze the visual content.
 * 3. Generates summary and tags based on what's visually seen in the PDF.
 * 4. Uploads the metadata back to R2.
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
            
            // VISUAL APPROACH: Use vision model to analyze PDF visual content
            console.log(`Starting visual analysis of PDF ${objectName}`);
            
            // 1. Convert PDF to data URL for vision model
            const base64Data = arrayBufferToBase64(pdfBuffer);
            const dataUrl = `data:application/pdf;base64,${base64Data}`;
            
            // 2. Use vision model to analyze what's visually in the PDF
            console.log(`Analyzing PDF visual content with vision model`);
            const visionPrompt = `
            You are looking at the first page of a PDF document.
            
            Carefully analyze everything you can see in this document and provide:
            1. A detailed description of what you can visually see in this PDF
            2. Information about text content, images, charts, tables and any other visible elements
            3. The overall purpose and content of this document based on what you observe
            
            Be specific and detailed about what you can actually see in the document.
            
            DO NOT include any formatting markers, prefixes, or phrases like "Here is my analysis:", "Summary:", etc.
            Just provide the direct content description.
            `;
            
            // Use vision model to analyze the PDF
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
            
            // 3. Process vision result to get visual content description
            let visualContent = "";
            if (typeof visionResult === 'string') {
                visualContent = visionResult;
            } else if (visionResult && visionResult.response) {
                visualContent = visionResult.response;
            } else if (visionResult && visionResult.content) {
                visualContent = visionResult.content;
            } else {
                throw new Error("Invalid vision model response format");
            }
            
            console.log(`Visual content extracted (${visualContent.length} chars)`);
            
            // 4. Generate summary and tags based on visual content
            const summaryPrompt = `
            I've visually analyzed a PDF document and here's what I can see:
            
            ${visualContent.substring(0, 6000)}
            
            IMPORTANT: You must follow these exact formatting instructions:
            
            1. Give me ONLY a plain text summary (1-2 paragraphs) with no formatting.
            2. After the summary, write "TAGS:" followed immediately by a comma-separated list of tags.
            
            FORMAT RULES:
            - NO introduction phrases like "Here is the response:" or "Summary:"
            - NO asterisks or other markdown formatting (**, *, etc.)
            - NO quotation marks around the summary or tags
            - NO newlines except between paragraphs and before the TAGS section
            - NO header text of any kind
            
            Your response must start directly with the summary text and nothing else.
            
            CORRECT EXAMPLE:
            This document is a financial report for Q2 2023. It contains quarterly revenue figures, expense breakdowns, and projections for the next quarter. The report includes several bar charts comparing performance metrics across departments.
            
            TAGS: financial, quarterly report, revenue, expenses, projections, charts, q2 2023`;
            
            const summaryResult = await ai.run('@cf/meta/llama-3-8b-instruct', {
                messages: [
                    { 
                        role: 'system', 
                        content: 'You are a document metadata specialist. Your job is to follow the EXACT formatting instructions without deviation. Never add phrases like "Here is the response" or "Summary:", or any formatting markers like asterisks. Respond with ONLY the requested content in the exact format specified.'
                    },
                    { role: 'user', content: summaryPrompt }
                ]
            }) as any;
            
            // 5. Process the summary result
            let summary = "";
            if (typeof summaryResult === 'string') {
                summary = summaryResult;
            } else if (summaryResult && summaryResult.response) {
                summary = summaryResult.response;
            } else {
                throw new Error("Failed to generate summary from visual content");
            }
            
            // 6. Extract tags and clean summary
            const tags = extractTags(summary);
            summary = cleanSummary(summary);
            
            // 7. Create metadata JSON
            let metadata = {
                filename: objectName,
                type: "pdf",
                summary: summary,
                tags: tags,
                size: object.size,
                lastModified: object.uploaded,
                generatedAt: new Date().toISOString(),
            };
            
            // Validate and clean the metadata before saving
            metadata = validateAndCleanOutput(metadata);

            // 8. Upload metadata back to R2
            const metadataFilename = `${objectName}.metadata.json`;
            await env.MEDIA_BUCKET.put(metadataFilename, JSON.stringify(metadata, null, 2), {
                httpMetadata: { contentType: 'application/json' },
            });

            console.log(`<- Successfully generated visual metadata for PDF ${objectName}`);
            
        } catch (aiError: any) {
            console.error(`AI vision processing error for PDF ${objectName}:`, aiError);
            
            // If vision processing fails, try a text-based approach as fallback
            try {
                console.log(`Trying text-based fallback for ${objectName}`);
                const { summary, tags } = await generateTextBasedMetadata(objectName, pdfBuffer, env.AI);
                
                // Create and upload metadata
                const metadata = {
                    filename: objectName,
                    type: "pdf",
                    summary: summary,
                    tags: tags,
                    size: object.size,
                    lastModified: object.uploaded,
                    generatedAt: new Date().toISOString(),
                };
                
                const metadataFilename = `${objectName}.metadata.json`;
                await env.MEDIA_BUCKET.put(metadataFilename, JSON.stringify(metadata, null, 2), {
                    httpMetadata: { contentType: 'application/json' },
                });
                
                console.log(`<- Generated text-based fallback metadata for PDF ${objectName}`);
                
            } catch (fallbackError) {
                // If both approaches fail, create minimal metadata
                console.error(`Fallback processing also failed for PDF ${objectName}:`, fallbackError);
                
                const filenameWords = objectName
                    .replace(/\.pdf$/i, '')
                    .split(/[_\-\s.]+/)
                    .filter(word => word.length > 2);
                    
                const metadata = {
                    filename: objectName,
                    type: "pdf",
                    summary: `PDF processing failed: ${aiError.message || "Unknown error"}`,
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
                
                console.log(`<- Generated minimal metadata for PDF ${objectName} due to processing errors`);
            }
        }

    } catch (error) {
        console.error(`Error processing PDF ${objectName}:`, error);
    }
}

/**
 * Convert ArrayBuffer to Base64 string
 */
function arrayBufferToBase64(buffer: ArrayBuffer): string {
    // Take the first 2MB max to avoid memory issues
    const maxBytes = 2 * 1024 * 1024;
    const limitedBuffer = buffer.slice(0, Math.min(buffer.byteLength, maxBytes));
    const bytes = new Uint8Array(limitedBuffer);
    
    // Efficiently convert to base64
    let binary = '';
    const chunkSize = 1024;
    
    for (let i = 0; i < bytes.length; i += chunkSize) {
        const chunk = bytes.subarray(i, Math.min(i + chunkSize, bytes.length));
        binary += String.fromCharCode.apply(null, Array.from(chunk));
    }
    
    return btoa(binary);
}

/**
 * Extract tags from a summary text
 */
function extractTags(text: string): string[] {
    // Default tags if extraction fails
    let tags = ["pdf"];
    
    // Look for tags section
    const tagMatch = text.match(/TAGS:(.+?)($|(?:\n\n))/s);
    
    if (tagMatch && tagMatch[1]) {
        // Extract and clean tags
        const rawTags = tagMatch[1].trim().split(/[,;]/).map(tag => {
            // Clean up each tag - remove asterisks, quotes, and other formatting markers
            return tag.trim()
                .toLowerCase()
                .replace(/^\*+|\*+$/g, '') // Remove asterisks at start/end
                .replace(/^"+|"+$/g, '')   // Remove quotes at start/end
                .replace(/^'|'$/g, '')     // Remove single quotes at start/end
                .replace(/^\[|\]$/g, '');  // Remove brackets at start/end
        });
        
        // Filter out empty tags and add to default tags
        const extractedTags = rawTags.filter(tag => tag.length > 0);
        if (extractedTags.length > 0) {
            tags = [...new Set(['pdf', ...extractedTags])];
        }
    }
    
    return tags;
}

/**
 * Clean summary text by removing tags section and formatting markers
 */
function cleanSummary(text: string): string {
    // First, extract everything before the TAGS: section
    let summary = text;
    const tagsIndex = text.toUpperCase().indexOf('TAGS:');
    if (tagsIndex !== -1) {
        summary = text.substring(0, tagsIndex).trim();
    }
    
    // Remove common formatting patterns
    summary = summary
        // Remove prefix phrases
        .replace(/^(?:here is the response:?|here's the response:?|my response:?)/i, '')
        .replace(/^(?:summary:?|content:?|description:?|analysis:?)/i, '')
        
        // Remove markdown and formatting
        .replace(/\*\*Summary:?\*\*/gi, '')
        .replace(/\*\*Summary \([^)]+\):?\*\*/gi, '')
        .replace(/\*\*/g, '')  // Remove all remaining double asterisks
        .replace(/\*/g, '')    // Remove all remaining single asterisks
        
        // Fix newlines and spacing
        .replace(/^\s+/gm, '')  // Remove leading whitespace from each line
        .replace(/\n{3,}/g, '\n\n')  // Replace 3+ consecutive newlines with just 2
        .trim();
    
    return summary;
}

// Add this function to perform a final validation of our processed output
function validateAndCleanOutput(metadata: any): any {
    // Clone the metadata object
    const cleanedMetadata = { ...metadata };
    
    // Handle summary
    if (typeof cleanedMetadata.summary === 'string') {
        // Remove any remaining formatting markers we might have missed
        let summary = cleanedMetadata.summary;
        
        // Check for common issues
        if (summary.includes("Here is") || 
            summary.includes("Summary:") || 
            summary.includes("**") ||
            summary.startsWith("\n")) {
            
            // Apply more aggressive cleaning
            summary = summary
                .replace(/^[\s\n]*(?:here is|here's)[^:\n]*:?[\s\n]*/i, '')
                .replace(/^[\s\n]*(?:\*\*)?summary(?:\*\*)?:?[\s\n]*/i, '')
                .replace(/\*\*[^*]*\*\*/g, '')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
        }
        
        cleanedMetadata.summary = summary;
    }
    
    // Handle tags
    if (Array.isArray(cleanedMetadata.tags)) {
        // Clean each tag
        cleanedMetadata.tags = cleanedMetadata.tags.map(tag => {
            if (typeof tag === 'string') {
                return tag
                    .replace(/^\*\*|\*\*$/g, '')  // Remove ** markers
                    .replace(/^""|""$/g, '')      // Remove "" markers
                    .trim();
            }
            return tag;
        });
        
        // Ensure no duplicate tags
        cleanedMetadata.tags = [...new Set(cleanedMetadata.tags)];
    }
    
    return cleanedMetadata;
}

/**
 * Generate metadata using text-based approach as fallback
 */
async function generateTextBasedMetadata(filename: string, pdfBuffer: ArrayBuffer, ai: Ai): Promise<{summary: string, tags: string[]}> {
    // Try to extract text content from PDF bytes
    const bytes = new Uint8Array(pdfBuffer.slice(0, 1024));
    const hexBytes = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join(' ');
    
    const extractionPrompt = `
    You're examining a PDF document named "${filename}".
    Here are the first few bytes in hex: ${hexBytes}
    
    Based on these bytes and your knowledge of PDF structure:
    1. What kind of document is this likely to be?
    2. Can you identify any text content, structure, or metadata from these bytes?
    3. What's the likely purpose and content of this document?
    
    Provide your best analysis of what this document contains.
    `;
    
    const extractionResult = await ai.run('@cf/meta/llama-3-8b-instruct', {
        messages: [
            { role: 'system', content: 'You are an expert at analyzing and understanding document structure from binary data.' },
            { role: 'user', content: extractionPrompt }
        ]
    }) as any;
    
    let extractedContent = "";
    if (typeof extractionResult === 'string') {
        extractedContent = extractionResult;
    } else if (extractionResult && extractionResult.response) {
        extractedContent = extractionResult.response;
    } else {
        throw new Error("Failed to extract content in fallback mode");
    }
    
    // Generate summary from extraction attempt
    const summaryPrompt = `
    Based on analysis of a PDF document named "${filename}", here's what I can determine:
    
    ${extractedContent}
    
    Please provide:
    1. A concise 1-2 paragraph summary of what this document likely contains
    2. A list of 5-8 relevant tags for this document
    
    Format your response as:
    [Summary paragraphs]
    
    TAGS: tag1, tag2, tag3, tag4, tag5
    `;
    
    const summaryResult = await ai.run('@cf/meta/llama-3-8b-instruct', {
        messages: [
            { role: 'system', content: 'You are a document specialist who creates accurate metadata for files.' },
            { role: 'user', content: summaryPrompt }
        ]
    }) as any;
    
    let summary = "";
    if (typeof summaryResult === 'string') {
        summary = summaryResult;
    } else if (summaryResult && summaryResult.response) {
        summary = summaryResult.response;
    } else {
        throw new Error("Failed to generate summary in fallback mode");
    }
    
    // Extract tags
    const tags = extractTags(summary);
    
    // Clean summary
    summary = cleanSummary(summary);
    
    return { summary, tags };
} 