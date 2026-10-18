import mongoose from 'mongoose';

const chatbotSchema = new mongoose.Schema(
    {
        user_id: {
            type: mongoose.Schema.Types.ObjectId,
            ref: 'User',
            required: true,
            index: true
        },
        workspace_id: {
            type: mongoose.Schema.Types.ObjectId,
            ref: 'Workspace',
            index: true,
            default: null
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
        category: {
            type: String,
            enum: ['ecommerce', 'customer_support', 'erp_crm', 'custom_api'],
            default: 'customer_support'
        },
        custom_api: {
            enabled: { type: Boolean, default: false },
            api_base_url: { type: String, trim: true, default: '' },
            headers: { type: mongoose.Schema.Types.Mixed, default: {} },
            lookup_endpoint: { type: String, trim: true, default: '' },
            action_endpoint: { type: String, trim: true, default: '' },
            action_name: { type: String, trim: true, default: '' },
            instructions: { type: String, trim: true, default: '' }
        },
        ecommerce: {
            enabled: { type: Boolean, default: false },
            store_url: { type: String, trim: true, default: '' },
            api_base_url: { type: String, trim: true, default: '' },
            image_base_url: { type: String, trim: true, default: '' },
            product_search_path: { type: String, trim: true, default: '' },
            product_detail_path: { type: String, trim: true, default: '' },
            order_checkout_path: { type: String, trim: true, default: '' },
            order_track_url: { type: String, trim: true, default: '' },
            support_hotline: { type: String, trim: true, default: '' },
            shipping_inside_dhaka: { type: Number, default: 0 },
            shipping_outside_dhaka: { type: Number, default: 0 },
            currency: { type: String, trim: true, default: '' },
            payment_methods: { type: [String], default: [] }
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
