import mongoose from 'mongoose';

const chatbotSchema = new mongoose.Schema(
    {
        user_id: {
            type: mongoose.Schema.Types.ObjectId,
            ref: 'User',
            required: true,
            index: true
        },
        created_by: {
            type: mongoose.Schema.Types.ObjectId,
            ref: 'User'
        },
        name: {
            type: String,
            required: true,
            trim: true
        },
        ai_model: {
            type: mongoose.Schema.Types.ObjectId,
            ref: 'AIModel',
            required: true
        },
        api_key: {
            type: String,
            required: true,
            trim: true
        },
        business_name: {
            type: String,
            trim: true,
            default: ''
        },
        business_description: {
            type: String,
            trim: true,
            default: ''
        },
        training_data: [
            {
                question: { type: String, trim: true },
                answer: { type: String, trim: true },
                context: { type: String, trim: true }
            }
        ],
        raw_training_text: {
            type: String,
            trim: true,
            default: ''
        },
        system_prompt: {
            type: String,
            trim: true,
            default: ''
        },
        status: {
            type: String,
            enum: ['active', 'inactive'],
            default: 'active'
        },
        knowledge_type: {
            type: String,
            enum: ['text', 'q&a', 'website_url', 'document'],
            default: 'text'
        },
        tone: {
            type: String,
            enum: ['professional', 'casual', 'friendly', 'humorous', 'empathetic', 'direct'],
            default: 'professional'
        },
        enable_human_handoff: {
            type: Boolean,
            default: false
        },
        message_limit: {
            type: Number,
            default: 0 
        },
        handoff_keywords: {
            type: [String],
            default: []
        },
        handoff_message: {
            type: String,
            default: "I'm connecting you with a human agent now. Someone will be with you shortly."
        },
        enable_google_meet: {
            type: Boolean,
            default: false
        },
        voice_settings: {
            type: mongoose.Schema.Types.Mixed,
            default: {}
        },
        deleted_at: {
            type: Date,
            default: null
        }
    },
    {
        timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
        collection: 'chatbots'
    }
);

chatbotSchema.index({ user_id: 1, status: 1 });

export default mongoose.model('Chatbot', chatbotSchema);
