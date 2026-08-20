import fs from 'fs';
import path from 'path';

export const transcribeAudio = async (filePath, apiKey, aiModel = null) => {
    try {
        let fileBuffer;
        let fileName;

        if (filePath.startsWith('http://') || filePath.startsWith('https://')) {
            const urlResponse = await fetch(filePath);
            if (!urlResponse.ok) throw new Error(`Failed to download audio from URL: ${urlResponse.statusText}`);
            const arrayBuffer = await urlResponse.arrayBuffer();
            fileBuffer = Buffer.from(arrayBuffer);
            fileName = filePath.split('/').pop().split('?')[0] || 'audio.ogg';
        } else {
            if (!fs.existsSync(filePath)) {
                throw new Error(`File not found: ${filePath}`);
            }
            fileBuffer = fs.readFileSync(filePath);
            fileName = path.basename(filePath);
        }
               const activeProvider = (aiModel?.provider || 'openai').toLowerCase();
        
        let finalApiKey = apiKey;
        if (activeProvider === 'google' || !finalApiKey || !finalApiKey.startsWith('sk-')) {
            finalApiKey = process.env.OPENAI_API_KEY || apiKey;
            if (activeProvider === 'google') {
                console.warn('[VoiceAI] Google Cloud APIs do not support API Keys. Transcribing via default OpenAI compatible endpoint using Master Key.');
            }
        }

        let apiEndpoint = aiModel?.api_endpoint || 'https://api.openai.com/v1/chat/completions';
        if (apiEndpoint.includes('/chat/completions')) {
            apiEndpoint = apiEndpoint.replace('/chat/completions', '/audio/transcriptions');
        } else if (!apiEndpoint.includes('/audio/transcriptions')) {
            apiEndpoint = 'https://api.openai.com/v1/audio/transcriptions';
        }

        const formData = new FormData();
        const blob = new Blob([fileBuffer], { type: 'audio/ogg' });
        formData.append('file', blob, fileName);
        formData.append('model', 'whisper-1');

        const response = await fetch(apiEndpoint, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${finalApiKey}`
            },
            body: formData
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error?.message || `Failed to transcribe audio`);
        }

        const data = await response.json();
        return data.text;

    } catch (error) {
        console.error('[VoiceAI] Transcription error:', error);
        return null;
    }
};

export const generateSpeech = async (text, voiceId, apiKey, aiModel = null, provider = null, outputDir = null) => {
    try {
        let response;
        const activeProvider = (provider || aiModel?.provider || 'openai').toLowerCase();

        if (activeProvider === 'elevenlabs') {
            const elevenLabsKey = process.env.ELEVENLABS_API_KEY || apiKey;
            response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId || '21m00Tcm4TlvDq8ikWAM'}`, {
                method: 'POST',
                headers: {
                    'xi-api-key': elevenLabsKey,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    text: text,
                    model_id: 'eleven_multilingual_v2'
                })
            });
        } else {
            let finalApiKey = apiKey;
            if (activeProvider === 'google' || !finalApiKey || !finalApiKey.startsWith('sk-')) {
                finalApiKey = process.env.OPENAI_API_KEY || apiKey;
                if (activeProvider === 'google') {
                    console.warn('[VoiceAI] Google Cloud APIs do not support API Keys. Defaulting to OpenAI compatible endpoint using Master Key.');
                }
            }
            
            let apiEndpoint = aiModel?.api_endpoint || 'https://api.openai.com/v1/chat/completions';
            if (apiEndpoint.includes('/chat/completions')) {
                apiEndpoint = apiEndpoint.replace('/chat/completions', '/audio/speech');
            } else if (!apiEndpoint.includes('/audio/speech')) {
                apiEndpoint = 'https://api.openai.com/v1/audio/speech';
            }

            response = await fetch(apiEndpoint, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${finalApiKey}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    model: 'tts-1',
                    input: text,
                    voice: voiceId || 'alloy',
                    response_format: 'mp3'
                })
            });
        }

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Failed to generate speech: ${errorText}`);
        }

        const arrayBuffer = await response.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);

        if (outputDir) {
            if (!fs.existsSync(outputDir)) {
                fs.mkdirSync(outputDir, { recursive: true });
            }

            const fileName = `tts_${Date.now()}.mp3`;
            const filePath = path.join(outputDir, fileName);
            fs.writeFileSync(filePath, buffer);

            return { buffer, filePath, fileName };
        }

        return { buffer, fileName: `tts_${Date.now()}.mp3` };
    } catch (error) {
        console.error('[VoiceAI] Speech generation error:', error);
        return null;
    }
};
