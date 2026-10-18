import db from '../models/index.js';
import unifiedWhatsAppService from '../services/whatsapp/unified-whatsapp.service.js';
import { callAIModel } from './ai-utils.js';
import mongoose from 'mongoose';
import axios from 'axios';

const {
    WorkingHours,
    MessageBot,
    WabaConfiguration,
    ReplyMaterial,
    Template,
    EcommerceCatalog,
    Chatbot,
    User,
    ChatAssignment,
    Contact,
    WhatsappPhoneNumber,
    Form,
    Submission,
    TelegramConnection,
    FacebookConnection,
    InstagramConnection,
    Message,
    Sequence,
    WhatsappWaba
} = db;


export const isWithinWorkingHours = async (wabaId) => {
    try {
        const workingHours = await WorkingHours.findOne({ waba_id: wabaId });
        if (!workingHours) return true;
        if (workingHours.is_holiday_mode) return false;

        const now = new Date();
        const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
        const currentDay = days[now.getDay()];
        const dayConfig = workingHours[currentDay];

        if (!dayConfig || dayConfig.status === 'closed') return false;
        if (!dayConfig.hours || dayConfig.hours.length === 0) return true;

        const currentTime = now.getHours().toString().padStart(2, '0') + ':' + now.getMinutes().toString().padStart(2, '0');

        return dayConfig.hours.some(range => {
            return currentTime >= range.from && currentTime <= range.to;
        });
    } catch (error) {
        console.error('Error checking working hours:', error);
        return true;
    }
};

export const findMatchingBot = async (accountId, text, platform = 'whatsapp', contact = null) => {
    if (!text) return null;

    let queryPlatforms = [platform];
    if (platform === 'whatsapp' || platform === 'baileys') {
        queryPlatforms = ['whatsapp', 'baileys'];
    }

    const bots = await MessageBot.find({
        account_id: accountId,
        status: 'active',
        deleted_at: null,
        $or: [
            { platform: { $in: queryPlatforms } },
            { platform: 'all' },
            { platform: null }
        ]
    });

    const normalizedText = text.toLowerCase().trim();

    for (const bot of bots) {
        if (bot.recipient_type && bot.recipient_type !== 'all_contacts') {
            if (!contact) {
                continue;
            }
            if (bot.recipient_type === 'specific_contacts') {
                const contactIdStr = contact._id.toString();
                const isIncluded = bot.specific_contacts && bot.specific_contacts.some(cId => cId.toString() === contactIdStr);
                if (!isIncluded) continue;
            } else if (bot.recipient_type === 'segments') {
                const contactSegments = contact.segments || [];
                const botSegments = bot.segment_ids || [];
                if (botSegments.length > 0) {
                    const hasSegment = botSegments.some(segId =>
                        contactSegments.some(cSegId => cSegId.toString() === segId.toString())
                    );
                    if (!hasSegment) continue;
                }
            } else if (bot.recipient_type === 'tags') {
                const contactTags = contact.tags || [];
                const botTags = bot.tag_ids || [];
                if (botTags.length > 0) {
                    const hasTag = botTags.some(tagId =>
                        contactTags.some(cTagId => cTagId.toString() === tagId.toString())
                    );
                    if (!hasTag) {
                        const hasContactTag = await db.ContactTag.findOne({
                            contact_id: contact._id,
                            tag_id: { $in: botTags }
                        }).lean();
                        if (!hasContactTag) continue;
                    }
                }
            }
        }
        if (bot.reply_type === 'template' || bot.reply_type_ref === 'Template') {
            const template = await Template.findById(bot.reply_id).lean();
            if (template) {
                const templatePlatform = template.platform || 'whatsapp';
                const isPlatformMatch = templatePlatform === platform || (['whatsapp', 'baileys'].includes(templatePlatform) && ['whatsapp', 'baileys'].includes(platform)) || ['telegram', 'facebook', 'instagram'].includes(platform);
                if (!isPlatformMatch) {
                    continue;
                }
            }
        }

        if (bot.reply_type === 'sequence') {
            const sequence = await Sequence.findById(bot.reply_id).lean();
            if (sequence) {
                const sequencePlatform = sequence.platform || 'whatsapp';
                const isPlatformMatch = sequencePlatform === platform || (['whatsapp', 'baileys'].includes(sequencePlatform) && ['whatsapp', 'baileys'].includes(platform)) || ['telegram', 'facebook', 'instagram'].includes(platform);
                if (!isPlatformMatch) {
                    continue;
                }
            }
        }

        for (const keyword of bot.keywords) {
            const normalizedKeyword = keyword.toLowerCase().trim();
            let matched = false;

            switch (bot.matching_method) {
                case 'exact':
                    if (normalizedText === normalizedKeyword) matched = true;
                    break;
                case 'contains':
                    if (normalizedText.includes(normalizedKeyword)) matched = true;
                    break;
                case 'starts_with':
                    if (normalizedText.startsWith(normalizedKeyword)) matched = true;
                    break;
                case 'ends_with':
                    if (normalizedText.endsWith(normalizedKeyword)) matched = true;
                    break;
                case 'partial':
                    const matchCount = [...normalizedKeyword].filter(char => normalizedText.includes(char)).length;
                    const percentage = (matchCount / normalizedKeyword.length) * 100;
                    if (percentage >= (bot.partial_percentage || 70)) matched = true;
                    break;
            }

            if (matched) return bot;
        }
    }
    return null;
};

export const sendOmnichannelMessageHelper = async ({ contactDoc, messageType, text, fileUrl, buttons, location, isTemplate, templateId, templateObj }) => {
    try {
        const platform = contactDoc.source;
        const userId = contactDoc.user_id;
        let workspaceId = contactDoc.workspace_id;
        let senderId = null;

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');

        let msgType = messageType || 'text';
        if (msgType === 'template') {
            msgType = 'text';
        }

        let fileUrlToPass = fileUrl;
        let dbFileUrl = fileUrl;

        if (platform === 'telegram') {
            const bot = await TelegramConnection.findOne({ user_id: contactDoc.user_id, is_active: true });
            if (!bot) {
                console.error('No active Telegram bot found for this contact');
                return;
            }
            senderId = bot.bot_id;
            workspaceId = workspaceId || bot.workspace_id;

            await omnichannelService.sendMessage({
                platform,
                workspace_id: workspaceId,
                user_id: userId,
                recipient_id: contactDoc.telegram_chat_id,
                message_type: msgType,
                text,
                file_url: fileUrlToPass,
                buttons,
                latitude: location?.latitude,
                longitude: location?.longitude,
                name: location?.name,
                address: location?.address
            });
        } else {
            let connection = null;
            if (platform === 'facebook') {
                connection = await FacebookConnection.findOne({ $or: [{ workspace_id: workspaceId }, { user_id: contactDoc.user_id }], is_active: true }).lean();
            } else if (platform === 'instagram') {
                connection = await InstagramConnection.findOne({ $or: [{ workspace_id: workspaceId }, { user_id: contactDoc.user_id }], is_active: true }).lean();
            }

            if (!connection) {
                console.error(`No active ${platform} connection for this workspace`);
                return;
            }
            workspaceId = workspaceId || connection.workspace_id;

            let pageId = null;
            const lastMsg = await Message.findOne({ contact_id: contactDoc._id }).sort({ wa_timestamp: -1 }).lean();
            if (lastMsg) {
                pageId = lastMsg.direction === 'inbound' ? lastMsg.recipient_id : lastMsg.sender_id;
            }

            if (platform === 'facebook') {
                if (!pageId) {
                    const page = connection.pages?.find(p => p.is_active !== false);
                    pageId = page?.page_id;
                }
                if (!pageId) {
                    console.error(`No active Facebook Page found for this workspace`);
                    return;
                }
                senderId = pageId;
            } else if (platform === 'instagram') {
                if (!pageId) {
                    pageId = connection.ig_user_id || (connection.pages && connection.pages[0]?.instagram_account_id);
                }
                if (!pageId) {
                    console.error(`No active Instagram Account found for this workspace`);
                    return;
                }

                const matchedPage = connection.pages?.find(p => p.page_id === pageId || p.instagram_account_id === pageId);
                if (matchedPage?.instagram_account_id) {
                    senderId = matchedPage.instagram_account_id;
                } else {
                    senderId = pageId;
                }
            }

            await omnichannelService.sendMessage({
                platform,
                workspace_id: workspaceId,
                user_id: userId,
                page_id: pageId,
                recipient_id: platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id,
                message_type: msgType,
                text,
                file_url: fileUrlToPass,
                buttons,
                latitude: location?.latitude,
                longitude: location?.longitude,
                name: location?.name,
                address: location?.address
            });
        }

        const newMessage = await Message.create({
            workspace_id: workspaceId,
            user_id: userId,
            contact_id: contactDoc._id,
            platform: platform,
            provider: platform,
            sender_id: senderId,
            recipient_id: platform === 'telegram' ? contactDoc.telegram_chat_id : (platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id),
            direction: 'outbound',
            message_type: isTemplate ? 'template' : msgType,
            template_id: isTemplate ? templateId : undefined,
            content: text,
            file_url: dbFileUrl,
            from_me: true,
            delivery_status: 'delivered',
            read_status: 'unread',
            wa_timestamp: new Date()
        });

        const io = unifiedWhatsAppService.io;
        if (io) {
            const formattedMessage = {
                id: newMessage._id.toString(),
                content: newMessage.content,
                interactiveData: newMessage.interactive_data || null,
                messageType: newMessage.message_type,
                fileUrl: newMessage.file_url || null,
                template: templateObj || null,
                createdAt: newMessage.wa_timestamp,
                can_chat: true,
                delivered_at: new Date(),
                delivery_status: newMessage.delivery_status || 'delivered',
                direction: newMessage.direction || 'outbound',
                sender: {
                    id: senderId,
                    name: 'Agent'
                },
                recipient: {
                    id: platform === 'telegram' ? contactDoc.telegram_chat_id : (platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id),
                    name: contactDoc.name
                },
                user_id: newMessage.user_id?.toString(),
                contact_id: contactDoc._id.toString(),
                platform: platform,
                provider: platform
            };
            io.emit('whatsapp:message', formattedMessage);
        }
    } catch (err) {
        console.error('Error in sendOmnichannelMessageHelper:', err);
    }
};

export const getEcommerceLiveProduct = async (ecommerceConfig, slug) => {
    if (!ecommerceConfig || !ecommerceConfig.api_base_url) return null;
    const base = ecommerceConfig.api_base_url.replace(/\/$/, '');
    const detailPath = ecommerceConfig.product_detail_path ? ecommerceConfig.product_detail_path.replace(/\/$/, '') : '';
    const searchPath = ecommerceConfig.product_search_path ? ecommerceConfig.product_search_path.replace(/\/$/, '') : '';

    if (detailPath) {
        try {
            const res = await axios.get(`${base}${detailPath}/${encodeURIComponent(slug)}`, { timeout: 6000 });
            if (res.data?.data) return res.data.data;
        } catch (e) {
            // try searchPath if provided
        }
    }
    if (searchPath) {
        try {
            const res = await axios.get(`${base}${searchPath}?slug=${encodeURIComponent(slug)}`, { timeout: 6000 });
            return (res.data?.data || [])[0] || null;
        } catch (e) {
            return null;
        }
    }
    return null;
};

export const placeEcommerceOrder = async (ecommerceConfig, { name, phone, address, area, shipping_cost, products, notes }) => {
    if (!ecommerceConfig || !ecommerceConfig.api_base_url || !ecommerceConfig.order_checkout_path) {
        return { success: false, message: 'No store API or checkout path configured' };
    }
    const base = ecommerceConfig.api_base_url.replace(/\/$/, '');
    const checkoutPath = ecommerceConfig.order_checkout_path;

    try {
        const payload = {
            name,
            contact_number: phone,
            shipping_address: address,
            shipping_cost: Number(shipping_cost) || ecommerceConfig.shipping_inside_dhaka || 80,
            payment_method: 'Cash on Delivery',
            notes: notes || null,
            products: products.map(p => ({
                product_id: p.product_id,
                ...(p.product_variation_id || p.variant_id ? {
                    product_variation_id: p.product_variation_id || p.variant_id,
                    variant_id: p.product_variation_id || p.variant_id
                } : {}),
                ...(p.variation_snapshot ? { variation_snapshot: p.variation_snapshot } : {}),
                quantity: Number(p.quantity) || 1,
                price: Number(p.price)
            }))
        };

        const res = await axios.post(`${base}${checkoutPath}`, payload, {
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            timeout: 10000
        });

        if (res.data?.order) {
            return {
                success: true,
                order: res.data.order
            };
        }
        return { success: false, message: 'Order checkout did not return order object' };
    } catch (err) {
        console.error('[Ecommerce Order API] Error placing order:', err.response?.data || err.message);
        return { success: false, error: err.response?.data?.message || err.message };
    }
};

export const extractSearchQueries = (text) => {
    if (!text || typeof text !== 'string') return [];
    const queries = [];

    // 1. Quoted phrases e.g. "Multi-Pocket Casual Backpack"
    const quoteMatches = text.match(/["“'‘]([^"”'’]+)["”'’]/g);
    if (quoteMatches) {
        quoteMatches.forEach(m => {
            const clean = m.replace(/["“”'’]/g, '').trim();
            if (clean.length > 2) queries.push(clean);
        });
    }

    // 2. Preserve hyphenated phrases intact (e.g. "Multi-Pocket", "HA-20DX")
    const hyphenatedWords = text.match(/[a-zA-Z0-9]+-[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*/g);
    if (hyphenatedWords) {
        hyphenatedWords.forEach(hw => {
            if (hw.length > 3) queries.push(hw);
        });
    }

    // 3. Stop words in Banglish, Bengali, and English
    const stopWordsRegex = /\b(hi|hello|salam|assalamu\s+alaikum|kemon\s+achen|bhai|vai|bro|apnader|kache|ki|er|price|dam|koto|ache|ase|asa|achhe|dekte|dekhan|chai|order|korbo|amar|lagbe|eta|eita|aita|ata|oita|please|details|bolun|janan|nibo|nibi|hobe|available|stock|ace|tai|na|ha|ji)\b/gi;
    const bengaliStopWordsRegex = /[\s]*(আছে|দাম|কত|এইটা|এটা|ওটা|কি|ভাই|আপনাদের|কাছে|দেখান|দেখতে|চাই|অর্ডার|করবো|প্লিজ|বলুন|জানান|নিতে|হবে|স্টক)[\s]*/g;

    const cleanedText = text
        .replace(stopWordsRegex, ' ')
        .replace(bengaliStopWordsRegex, ' ')
        .replace(/[?.,!/\\#"“”'’`~@$%^&*()+=[\]{}|;:<>]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

    if (cleanedText.length > 2) {
        queries.push(cleanedText);
    }

    // 4. Extract distinctive content words (length >= 4 or capitalized)
    const words = cleanedText.split(' ').filter(w => w.length > 2);
    const distinctiveWords = [...words].sort((a, b) => {
        const aCap = /^[A-Z]/.test(a) ? 2 : 0;
        const bCap = /^[A-Z]/.test(b) ? 2 : 0;
        return (b.length + bCap) - (a.length + aCap);
    });

    distinctiveWords.slice(0, 4).forEach(w => queries.push(w));

    // 5. Multi-word phrase combinations
    if (words.length >= 2) {
        queries.push(words.slice(0, 2).join(' '));
        if (words.length >= 3) {
            queries.push(words.slice(1, 3).join(' '));
        }
    }

    return [...new Set(queries.filter(q => q && q.length > 1))];
};

export const scoreProductMatch = (productName, incomingText) => {
    if (!productName || !incomingText) return 0;
    const normName = productName.toLowerCase().replace(/[^a-z0-9]/g, ' ');
    const normInput = incomingText.toLowerCase().replace(/[^a-z0-9]/g, ' ');
    const nameWords = normName.split(/\s+/).filter(w => w.length > 2);
    const inputWords = normInput.split(/\s+/).filter(w => w.length > 2);

    let matches = 0;
    for (const iw of inputWords) {
        if (nameWords.includes(iw)) matches++;
    }
    return inputWords.length > 0 ? (matches / inputWords.length) : 0;
};

export const resolveProductVariation = (productDetails, userText, aiOrderData) => {
    let variations = productDetails?.variations;
    if (typeof variations === 'string') {
        try {
            variations = JSON.parse(variations);
        } catch (e) {
            variations = [];
        }
    }

    let resolvedId = aiOrderData?.product_variation_id || null;
    let resolvedSnapshot = aiOrderData?.variation_snapshot || null;

    if (Array.isArray(variations) && variations.length > 0) {
        // 1. If AI already provided a valid variation ID from the list
        if (resolvedId) {
            const found = variations.find(v => String(v.id) === String(resolvedId));
            if (found) {
                const parts = [];
                if (found.color) parts.push(`Color: ${found.color}`);
                if (found.attributes?.Size || found.size) parts.push(`Size: ${found.attributes?.Size || found.size}`);
                return {
                    variation_id: found.id,
                    variation_snapshot: parts.join(', ') || resolvedSnapshot || 'Selected variation'
                };
            }
        }

        // 2. Auto-match from user's conversation text and variation_snapshot
        let combinedText = `${resolvedSnapshot || ''} ${userText || ''}`.toLowerCase();
        
        const BENGALI_COLORS = {
            'হলুদ': 'yellow',
            'কালো': 'black',
            'লাল': 'red',
            'সাদা': 'white',
            'নীল': 'blue',
            'সবুজ': 'green',
            'মেরুন': 'maroon',
            'অলিভ': 'olive',
            'গোলাপি': 'pink',
            'গোলাপী': 'pink',
            'লেমন': 'lemon'
        };
        for (const [bn, en] of Object.entries(BENGALI_COLORS)) {
            if (combinedText.includes(bn)) {
                combinedText += ` ${en}`;
            }
        }

        // Pass A: Match both color and size
        for (const v of variations) {
            const color = (v.color || '').toLowerCase();
            const size = (v.attributes?.Size || v.size || '').toLowerCase();

            const colorMatches = color && combinedText.includes(color);
            const sizeMatches = size && (
                new RegExp(`\\b${size}\\b`, 'i').test(combinedText) ||
                combinedText.includes(`${size} size`) ||
                combinedText.includes(`size ${size}`) ||
                combinedText.includes(`সাইজ ${size}`)
            );

            if (colorMatches && sizeMatches) {
                const parts = [];
                if (v.color) parts.push(`Color: ${v.color}`);
                if (v.attributes?.Size || v.size) parts.push(`Size: ${v.attributes?.Size || v.size}`);
                return {
                    variation_id: v.id,
                    variation_snapshot: parts.join(', ')
                };
            }
        }

        // Pass B: Match color only
        for (const v of variations) {
            const color = (v.color || '').toLowerCase();
            if (color && combinedText.includes(color)) {
                const parts = [`Color: ${v.color}`];
                if (v.attributes?.Size || v.size) parts.push(`Size: ${v.attributes?.Size || v.size}`);
                return {
                    variation_id: v.id,
                    variation_snapshot: parts.join(', ')
                };
            }
        }

        // Pass C: Match size only
        for (const v of variations) {
            const size = (v.attributes?.Size || v.size || '').toLowerCase();
            if (size && (new RegExp(`\\b${size}\\b`, 'i').test(combinedText) || combinedText.includes(`সাইজ ${size}`))) {
                const parts = [];
                if (v.color) parts.push(`Color: ${v.color}`);
                parts.push(`Size: ${v.attributes?.Size || v.size}`);
                return {
                    variation_id: v.id,
                    variation_snapshot: parts.join(', ')
                };
            }
        }
    }

    // 3. Fallback: If snapshot is not yet set, extract any explicit color / size mentioned in conversation
    if (!resolvedSnapshot && userText) {
        const textLower = userText.toLowerCase();
        const extractedParts = [];
        const colorMap = {
            'yellow': 'Yellow', 'black': 'Black', 'white': 'White', 'red': 'Red',
            'blue': 'Blue', 'green': 'Green', 'maroon': 'Maroon', 'olive': 'Olive',
            'হলুদ': 'Yellow', 'কালো': 'Black', 'লাল': 'Red', 'সাদা': 'White', 'নীল': 'Blue', 'সবুজ': 'Green'
        };
        for (const [k, v] of Object.entries(colorMap)) {
            if (textLower.includes(k)) {
                extractedParts.push(`Color: ${v}`);
                break;
            }
        }
        const sizeMatch = textLower.match(/\b(xxl|xl|l|m|s|xs)\b/i) || textLower.match(/সাইজ\s*([a-zA-Z0-9]+)/i);
        if (sizeMatch && sizeMatch[1]) {
            extractedParts.push(`Size: ${sizeMatch[1].toUpperCase()}`);
        }
        if (extractedParts.length > 0) {
            resolvedSnapshot = extractedParts.join(', ');
        }
    }

    return {
        variation_id: resolvedId,
        variation_snapshot: resolvedSnapshot
    };
};

export const formatProductContext = (product, ecommerceConfig) => {
    if (!product) return '';
    const curr = ecommerceConfig?.currency || '৳';
    const cleanDesc = (product.description || '').replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').slice(0, 350);

    let variations = product.variations;
    if (typeof variations === 'string') {
        try {
            variations = JSON.parse(variations);
        } catch (e) {
            variations = [];
        }
    }

    let variationText = '';
    if (variations && Array.isArray(variations) && variations.length > 0) {
        const varLines = variations.slice(0, 36).map(v => {
            const color = v.color ? `Color: ${v.color}` : '';
            const size = (v.attributes?.Size || v.size) ? `Size: ${v.attributes?.Size || v.size}` : '';
            const label = [color, size].filter(Boolean).join(', ');
            return `  * Variation ID: ${v.id} | ${label || 'Default'} | SKU: ${v.sku || 'N/A'}`;
        }).join('\n');
        variationText = `\n- Available Variations (Colors & Sizes):\n${varLines}`;
    }

    return `[ACTIVE PRODUCT IN THIS CONVERSATION]:
- Product ID: ${product.id}
- Product Name: ${product.name}
- Product SKU: ${product.product_sku || product.sku || 'N/A'}
- Regular Price: ${curr}${product.price}
- Offer / Sale Price: ${curr}${product.sale_price || product.price}
- Link: ${ecommerceConfig?.store_url ? `${ecommerceConfig.store_url.replace(/\/$/, '')}/products/${product.slug}` : product.slug}${variationText}
- Description: ${cleanDesc}`;
};

export const cleanReplyUrlsAndFormatting = (text, activeProduct = null) => {
    if (!text || typeof text !== 'string') return text;
    let cleaned = text;

    // 1. Separate numbered list items into distinct lines if bunched together
    cleaned = cleaned.replace(/([^\n])\s+([১-৯0-9]+\.\s+)/g, '$1\n\n$2');

    // 2. Unescape backslashes before parens or brackets (e.g. \) or \])
    cleaned = cleaned.replace(/\\([()[\]])/g, '$1');

    // 3. Convert markdown links [text](url) -> url or text url (stripping any malformed trailing brackets/parens)
    cleaned = cleaned.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/gi, (match, anchor, url) => {
        const cleanAnchor = anchor.replace(/[)\]]+$/, '').trim();
        const cleanUrl = url.replace(/[)\]]+$/, '').trim();
        if (cleanAnchor === cleanUrl || cleanAnchor.startsWith('http')) {
            return cleanUrl;
        }
        return `${cleanAnchor} ${cleanUrl}`;
    });

    // 4. Remove surrounding parentheses around links like "(লিংক: https://...)" or "(https://...)"
    cleaned = cleaned.replace(/\(([^)]*?)(https?:\/\/[^\s)]+)\)/gi, (match, prefix, url) => {
        const cleanPrefix = prefix ? prefix.trim() + ' ' : '';
        return ` ${cleanPrefix}${url.trim()} `;
    });

    // 5. Remove surrounding brackets around links like "[লিংক: https://...]" or "[https://...]"
    cleaned = cleaned.replace(/\[([^\]]*?)(https?:\/\/[^\s\]]+)\]/gi, (match, prefix, url) => {
        const cleanPrefix = prefix ? prefix.trim() + ' ' : '';
        return ` ${cleanPrefix}${url.trim()} `;
    });

    // 6. Fix singular /product/ to plural /products/ in store URLs
    cleaned = cleaned.replace(/(https?:\/\/[^\s/]+)\/product\/([a-zA-Z0-9_-]+)/gi, '$1/products/$2');

    // 7. If an active product is known and AI generated a link with missing numeric slug suffix, auto-correct the slug
    if (activeProduct && activeProduct.slug) {
        const realSlug = activeProduct.slug;
        const baseSlug = realSlug.replace(/-\d+$/, '');
        if (baseSlug && baseSlug !== realSlug) {
            const escapedBase = baseSlug.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const slugRegex = new RegExp(`(\\/products\\/)${escapedBase}(?!-\\d+)`, 'gi');
            cleaned = cleaned.replace(slugRegex, `$1${realSlug}`);
        }
    }

    // 8. Strip any trailing parenthesis or bracket directly attached to a URL
    cleaned = cleaned.replace(/(https?:\/\/[^\s)\]]+)[)\]]+/gi, '$1');

    // 10. Formal Corporate Tone: Replace informal "ভাই/ভাইয়া" with "স্যার"
    cleaned = cleaned.replace(/([ক-হঁ-ঢ়A-Za-z]+)\s+(?:ভাই|ভাইয়া|ভাইয়া)(?=[,!\s।?]|$)/gu, '$1 স্যার');
    cleaned = cleaned.replace(/(?:^|[\s,])(?:ভাই|ভাইয়া|ভাইয়া)(?=[,!\s।?]|$)/gu, (match) => {
        return match.replace(/ভাই(?:য়া|য়া)?/u, 'স্যার');
    });

    // 11. Clean up extra spaces on lines
    cleaned = cleaned.split('\n').map(line => line.replace(/[ \t]+/g, ' ').trim()).join('\n');

    return cleaned.trim();
};

export const getEcommerceLiveContext = async (ecommerceConfig, incomingText, recentHistoryText = '') => {
    if (!ecommerceConfig || !ecommerceConfig.enabled || !incomingText || typeof incomingText !== 'string') {
        return { context: '', matchedProduct: null };
    }
    const contextParts = [];

    const base = (ecommerceConfig.api_base_url || '').replace(/\/$/, '');
    const storeDomain = ecommerceConfig.store_url ? ecommerceConfig.store_url.replace(/^https?:\/\//i, '').replace(/\/.*$/, '') : '';

    // 1. Check for a direct product link or slug in incoming text first, then recent conversation
    let slug = null;
    const linkRegex = new RegExp(`(?:${storeDomain.replace('.', '\\.')})?\\/products?\\/([a-zA-Z0-9_-]+)`, 'i');
    const incomingMatch = incomingText.match(linkRegex);
    if (incomingMatch && incomingMatch[1]) {
        slug = incomingMatch[1];
    } else if (recentHistoryText) {
        const historyMatches = [...recentHistoryText.matchAll(new RegExp(`(?:${storeDomain.replace('.', '\\.')})?\\/products?\\/([a-zA-Z0-9_-]+)`, 'ig'))];
        if (historyMatches.length > 0) {
            slug = historyMatches[historyMatches.length - 1][1];
        }
    }

    let matchedProduct = null;
    if (slug) {
        matchedProduct = await getEcommerceLiveProduct(ecommerceConfig, slug);
        if (matchedProduct) {
            contextParts.push(formatProductContext(matchedProduct, ecommerceConfig));
        }
    }

    // 2. Product search inquiry
    const searchPath = ecommerceConfig.product_search_path ? ecommerceConfig.product_search_path.replace(/\/$/, '') : '';
    if (searchPath && base) {
        // Collect candidate queries from incoming text first, then recent customer messages
        let queriesToTry = extractSearchQueries(incomingText);
        if (queriesToTry.length === 0 && recentHistoryText) {
            const customerLines = recentHistoryText.split('\n')
                .filter(l => l.startsWith('Customer:'))
                .map(l => l.replace('Customer:', '').trim());
            for (let i = customerLines.length - 1; i >= 0; i--) {
                const qs = extractSearchQueries(customerLines[i]);
                if (qs.length > 0) {
                    queriesToTry = qs;
                    break;
                }
            }
        }

        if (queriesToTry.length > 0) {
            const candidateProducts = [];
            const seenIds = new Set();

            for (const q of queriesToTry) {
                try {
                    const res = await axios.get(`${base}${searchPath}?search=${encodeURIComponent(q)}`, { timeout: 5000 });
                    const products = res.data?.data || [];
                    for (const p of products) {
                        if (!seenIds.has(p.id)) {
                            seenIds.add(p.id);
                            candidateProducts.push(p);
                        }
                    }
                    if (candidateProducts.length >= 10) break;
                } catch (e) {
                    console.error('[Ecommerce Live Context] Error searching products:', e.message);
                }
            }

            if (candidateProducts.length > 0) {
                // Score and rank candidates by relevance to customer text
                candidateProducts.sort((a, b) => scoreProductMatch(b.name, incomingText) - scoreProductMatch(a.name, incomingText));

                const topProducts = candidateProducts.slice(0, 5);
                const curr = ecommerceConfig.currency || '৳';
                const productList = topProducts.map((p, idx) => 
                    `${idx + 1}. [ID: ${p.id}] ${p.name} — Price: ${curr}${p.sale_price || p.price}\n   Link: ${ecommerceConfig.store_url ? `${ecommerceConfig.store_url.replace(/\/$/, '')}/products/${p.slug}` : p.slug}`
                ).join('\n\n');

                contextParts.push(`[LIVE SEARCH RESULTS FROM STORE]:
${productList}
* INSTRUCTION: Use these real product IDs, names, and prices. When listing products, put EACH product on a new line. Output ONLY plain text links like: লিংক: https://store.com/products/slug`);

                // If not already matched by slug, load the highest-scoring candidate as the active product
                if (!matchedProduct && topProducts[0]?.slug) {
                    try {
                        const topDetail = await getEcommerceLiveProduct(ecommerceConfig, topProducts[0].slug);
                        matchedProduct = topDetail || topProducts[0];
                        if (matchedProduct) {
                            contextParts.unshift(formatProductContext(matchedProduct, ecommerceConfig));
                        }
                    } catch (e) {
                        matchedProduct = topProducts[0];
                    }
                }
            }
        }
    }

    // 3. Order tracking
    if (/(track|tracking|order koi|order status|kobe pabo|delivery kobe|ট্র্যাক|ট্র্যাকিং|অর্ডার কোথায়|কবে পাবো)/i.test(incomingText)) {
        if (ecommerceConfig.order_track_url || ecommerceConfig.support_hotline) {
            contextParts.push(`[ORDER TRACKING INSTRUCTION]:
- Tell the customer: "আপনার অর্ডার ট্র্যাক করতে এই লিংকে ভিজিট করুন: ${ecommerceConfig.order_track_url || ''} ${ecommerceConfig.support_hotline ? `অথবা আমাদের হেল্পলাইনে যোগাযোগ করুন: ${ecommerceConfig.support_hotline}` : ''}"`);
        }
    }

    return { context: contextParts.join('\n\n'), matchedProduct };
};

export const getCustomApiLiveContext = async (customApiConfig, incomingText) => {
    if (!customApiConfig || !customApiConfig.enabled || !customApiConfig.api_base_url || !customApiConfig.lookup_endpoint) return '';
    try {
        const base = customApiConfig.api_base_url.replace(/\/$/, '');
        const endpoint = customApiConfig.lookup_endpoint.startsWith('/') ? customApiConfig.lookup_endpoint : `/${customApiConfig.lookup_endpoint}`;
        const delimiter = endpoint.includes('?') ? '&' : '?';
        const url = `${base}${endpoint}${delimiter}q=${encodeURIComponent(incomingText)}`;
        
        const headers = {
            'Accept': 'application/json',
            ...(customApiConfig.headers || {})
        };
        const res = await axios.get(url, { headers, timeout: 6000 });
        if (res.data) {
            return `--- EXTERNAL SYSTEM / ERP DATA ---\n${JSON.stringify(res.data).substring(0, 1500)}`;
        }
    } catch (err) {
        console.warn('[Custom API Lookup] Error:', err.message);
    }
    return '';
};

export const executeCustomApiAction = async (customApiConfig, payload) => {
    if (!customApiConfig || !customApiConfig.api_base_url || !customApiConfig.action_endpoint) {
        return { success: false, message: 'Custom action endpoint not configured' };
    }
    try {
        const base = customApiConfig.api_base_url.replace(/\/$/, '');
        const endpoint = customApiConfig.action_endpoint.startsWith('/') ? customApiConfig.action_endpoint : `/${customApiConfig.action_endpoint}`;
        const headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            ...(customApiConfig.headers || {})
        };
        const res = await axios.post(`${base}${endpoint}`, payload, { headers, timeout: 10000 });
        return { success: true, data: res.data };
    } catch (err) {
        console.error('[Custom API Action] Execution error:', err.response?.data || err.message);
        return { success: false, error: err.response?.data || err.message };
    }
};

export const sendAutomatedReply = async (params) => {
    const { wabaId, contactId, replyType, replyId, incomingText, userId, whatsappPhoneNumberId, sequenceAutomationData } = params;

    try {
        const contactDoc = await Contact.findById(contactId);
        if (!contactDoc) {
            console.error(`Contact not found for automated reply: ${contactId}`);
            return;
        }

        let activeBot = params.bot;
        if (!activeBot && params.botId) {
            activeBot = await MessageBot.findById(params.botId).lean();
        }

        const isOmnichannel = ['telegram', 'facebook', 'instagram'].includes(contactDoc.source);

        if (isOmnichannel) {
            if (replyType === 'flow' || replyType === 'catalog' || replyType === 'EcommerceCatalog' || replyType === 'appointment_flow') {
                console.log(`[Omnichannel] Reply type '${replyType}' is not supported on ${contactDoc.source}`);
                return;
            }
            if (replyType === 'ReplyMaterial' || replyType === 'replymaterial') {
                const material = await ReplyMaterial.findById(replyId).lean();
                if (material && material.type === 'flow') {
                    console.log(`[Omnichannel] Reply material type 'flow' is not supported on ${contactDoc.source}`);
                    return;
                }
            }
            if (replyType === 'Template' || replyType === 'template') {
                const template = await Template.findById(replyId).lean();
                if (template) {
                    const templateType = (template.template_type || '').toLowerCase();
                    if (templateType === 'catalog' || templateType === 'carousel_product') {
                        console.log(`[Omnichannel] Template type '${templateType}' is not supported on ${contactDoc.source}`);
                        return;
                    }
                }
            }
        }

        if (replyType === 'flow' || replyType === 'ReplyMaterial' || replyType === 'replymaterial') {
            const material = await ReplyMaterial.findById(replyId).lean();
            if (material && material.type === 'flow') {
                const form = await Form.findOne({ "flow.flow_id": material.flow_id, deleted_at: null }).lean();
                if (form && form.submit_settings?.max_submissions_per_user > 0) {
                    const submissionCount = await Submission.countDocuments({
                        "meta.phone_number": params.senderNumber,
                        "meta.flow_id": material.flow_id
                    });

                    if (submissionCount >= form.submit_settings.max_submissions_per_user) {
                        const fallbackMessage = form.submit_settings.limit_exceeded_message || "You have already reached the maximum number of submissions for this form.";
                        if (isOmnichannel) {
                            await sendOmnichannelMessageHelper({
                                contactDoc,
                                messageType: 'text',
                                text: fallbackMessage
                            });
                        } else {
                            await unifiedWhatsAppService.sendMessage(userId, {
                                recipientNumber: params.senderNumber,
                                messageText: fallbackMessage,
                                messageType: 'text',
                                whatsappPhoneNumberId
                            });
                        }
                        return;
                    }
                }
            }
        }

        if (replyType === 'chatbot') {
            const chatbot = await Chatbot.findById(replyId).populate('ai_model');

            if (chatbot && chatbot.status === 'active') {
                if (contactDoc.chatbot_overrides) {
                    const overrides = contactDoc.chatbot_overrides;
                    if (overrides.tone) chatbot.tone = overrides.tone;
                    if (overrides.message_limit !== undefined) chatbot.message_limit = overrides.message_limit;
                    if (overrides.enable_human_handoff !== undefined) chatbot.enable_human_handoff = overrides.enable_human_handoff;
                    if (overrides.handoff_keywords) chatbot.handoff_keywords = overrides.handoff_keywords;
                    if (overrides.handoff_message) chatbot.handoff_message = overrides.handoff_message;
                }

                if (contactDoc.chatbot_paused) {
                    console.log(`[Chatbot] AI Agent is paused for contact ${contactDoc._id}, skipping.`);
                    return;
                }

                if (chatbot.message_limit > 0 && contactDoc.ai_message_count >= chatbot.message_limit) {
                    console.log(`[Chatbot] Message limit reached for contact ${contactDoc._id}`);
                    contactDoc.chatbot_paused = true;
                    contactDoc.assigned_chatbot = null;
                    await contactDoc.save();

                    const io = unifiedWhatsAppService.io;
                    if (io) {
                        io.emit('agent:escalation', {
                            contact_id: contactDoc._id.toString(),
                            workspace_id: contactDoc.workspace_id?.toString() || null,
                            user_id: userId,
                            message: "AI Agent reached maximum message limit",
                            timestamp: new Date().toISOString()
                        });
                    }

                    const handoffMessage = chatbot.handoff_message || "I'm connecting you with a human agent now. Someone will be with you shortly.";
                    if (isOmnichannel) {
                        await sendOmnichannelMessageHelper({
                            contactDoc,
                            messageType: 'text',
                            text: handoffMessage
                        });
                    } else {
                        await unifiedWhatsAppService.sendMessage(userId, {
                            recipientNumber: params.senderNumber,
                            messageText: handoffMessage,
                            messageType: 'text',
                            whatsappPhoneNumberId
                        });
                    }
                    return;
                }

                if (chatbot.enable_human_handoff && chatbot.handoff_keywords && chatbot.handoff_keywords.length > 0) {
                    const incomingTextLower = incomingText.toLowerCase();
                    const isKeywordHandoff = chatbot.handoff_keywords.some(kw => incomingTextLower.includes(kw.toLowerCase()));

                    if (isKeywordHandoff) {
                        console.log(`[Chatbot] Keyword Handoff triggered for contact ${contactDoc._id}`);
                        contactDoc.chatbot_paused = true;
                        contactDoc.assigned_chatbot = null;
                        await contactDoc.save();

                        const io = unifiedWhatsAppService.io;
                        if (io) {
                            io.emit('agent:escalation', {
                                contact_id: contactDoc._id.toString(),
                                workspace_id: contactDoc.workspace_id?.toString() || null,
                                user_id: userId,
                                message: "User requested human agent via keyword",
                                timestamp: new Date().toISOString()
                            });
                        }

                        const handoffMessage = chatbot.handoff_message || "I'm connecting you with a human agent now. Someone will be with you shortly.";
                        
                        if (isOmnichannel) {
                            await sendOmnichannelMessageHelper({
                                contactDoc,
                                messageType: 'text',
                                text: handoffMessage
                            });
                        } else {
                            await unifiedWhatsAppService.sendMessage(userId, {
                                recipientNumber: params.senderNumber,
                                messageText: handoffMessage,
                                messageType: 'text',
                                whatsappPhoneNumberId
                            });
                        }
                        return;
                    }
                }

                const ecommerce = chatbot.ecommerce && chatbot.ecommerce.enabled ? chatbot.ecommerce : null;

                // Fetch recent conversation history so the bot has memory of what was discussed
                let recentHistoryText = '';
                try {
                    const recentMsgs = await Message.find({ contact_id: contactDoc._id })
                        .sort({ wa_timestamp: -1 })
                        .limit(8)
                        .lean();
                    if (recentMsgs && recentMsgs.length > 0) {
                        recentHistoryText = recentMsgs.reverse().map(m => 
                            `${m.direction === 'inbound' ? 'Customer' : 'Assistant'}: ${m.content}`
                        ).join('\n');
                    }
                } catch (historyErr) {
                    console.error('[Chatbot] Error fetching recent messages history:', historyErr);
                }

                let liveContext = '';
                let activeMatchedProduct = null;
                const customApi = chatbot.custom_api && chatbot.custom_api.enabled ? chatbot.custom_api : null;

                if (ecommerce) {
                    const liveRes = await getEcommerceLiveContext(ecommerce, incomingText, recentHistoryText);
                    if (typeof liveRes === 'string') {
                        liveContext = liveRes;
                    } else if (liveRes && typeof liveRes === 'object') {
                        liveContext = liveRes.context || '';
                        activeMatchedProduct = liveRes.matchedProduct || null;
                    }
                } else if (customApi) {
                    liveContext = await getCustomApiLiveContext(customApi, incomingText);
                }

                let agentInstructions = `\n\n--- SYSTEM INSTRUCTIONS ---\nYou are an advanced AI Agent for customer support and sales.
You MUST respond with a valid JSON object only. Do not output markdown code blocks or any other text outside the JSON.
The JSON must have the following keys:
- "reply_text": (string) Your actual helpful, courteous response in the customer's language.`;

                if (ecommerce) {
                    const curr = ecommerce.currency || '৳';
                    const insideRate = ecommerce.shipping_inside_dhaka || 80;
                    const outsideRate = ecommerce.shipping_outside_dhaka || 120;
                    const trackUrl = ecommerce.order_track_url || '';
                    const hotline = ecommerce.support_hotline || '';

                    agentInstructions += `
- "action": (string) If and ONLY if:
  1) You have customer's Name, Phone, and Delivery Address,
  2) AND you have a VERIFIED "product_id" explicitly listed in [ACTIVE PRODUCT IN THIS CONVERSATION] or [LIVE SEARCH RESULTS FROM STORE].
  If ANY delivery detail or verified product_id is missing, action MUST be null. NEVER invent, guess, or reuse an old product_id from a different product!
- "order_data": (object or null) If action is "place_order", provide:
  {
    "name": (string) customer name,
    "phone": (string) 11 digit customer phone number (e.g. 01968402925),
    "address": (string) full delivery address,
    "area": (string) "Inside Dhaka" or "Outside Dhaka",
    "shipping_cost": (number) ${insideRate} for Inside Dhaka or ${outsideRate} for Outside,
    "product_id": (number) MUST be the exact numeric ID from [ACTIVE PRODUCT IN THIS CONVERSATION] or [LIVE SEARCH RESULTS FROM STORE],
    "product_variation_id": (number or null) If the product has variations (Colors/Sizes), specify the exact numeric Variation ID chosen by customer,
    "variation_snapshot": (string or null) E.g. "Color: Yellow, Size: M" representing customer's chosen color and size,
    "quantity": (number) quantity ordered (default 1),
    "price": (number) unit price of the product
  }

--- ORDER & CONVERSATION RULES ---
1. MEMORY & PRODUCT FOCUS:
   - Carefully read the "RECENT CONVERSATION HISTORY" above!
   - If the customer mentions a NEW product, focus ONLY on that new product! Never order an old product when the customer is discussing a new one!
   - If the customer already gave their name/number/address in a previous message, do not ask for them again.

2. STRICT ORDER PLACEMENT & VARIATION RULES:
   - You can ONLY trigger "place_order" if you know the EXACT "product_id" from the live store context above.
   - If the product lists "Available Variations (Colors & Sizes)" and the customer has NOT selected a color or size, DO NOT place the order yet! Ask the customer politely which color or size they prefer.
   - Once the customer chooses a color/size, match it to the available variations and set "product_variation_id" and "variation_snapshot" in "order_data".
   - When order is placed, confirm in "reply_text" with item name, chosen variation (if any), total price, and Cash on Delivery.
3. DELIVERY & PAYMENT:
   - Delivery Charges: Inside Dhaka ${curr}${insideRate}, Outside Dhaka ${curr}${outsideRate}.
   - Payment: Cash on Delivery (COD).

4. PRODUCT LISTING & URL FORMATTING:
   - When suggesting products or sharing links, put EACH item on its own separate line.
   - CRITICAL: Output ONLY raw plain-text URLs! WhatsApp does NOT support markdown links!
   - NEVER use markdown link formatting like [Product](https://...) or [https://...](https://...)!
   - NEVER wrap URLs inside parentheses like (https://...) or brackets [https://...]!
   - NEVER attach ")" or "]" or any punctuation directly to the end of a URL! Trailing characters break the link and cause 404 errors!
   - CRITICAL URL ACCURACY: Store product links ALWAYS start with "/products/" (PLURAL) and MUST contain the EXACT full slug with its ID number (e.g. https://valokichu.com/products/item-name-12345). NEVER use "/product/" (singular) and NEVER invent or strip numbers from slugs!
   - CRITICAL ANTI-HALLUCINATION: If a product is NOT present in [ACTIVE PRODUCT IN THIS CONVERSATION] or [LIVE SEARCH RESULTS FROM STORE], you DO NOT have it! NEVER invent a fake product URL, price, or product! Politely reply that you could not find the item in the store catalog and ask for the exact link.
   - Always format product listings with clean line breaks, for example:
     ১. Product Name - Price
     লিংক: https://store.com/products/slug

5. CORPORATE TONE & ADDRESSING (CRITICAL):
   - Always maintain a professional, polite, and corporate customer service tone at all times.
   - Address the customer respectfully as "স্যার" (Sir) or "[Name] স্যার" (e.g. "মোনায়েম স্যার").
   - NEVER use informal words like "ভাই" (bhai), "ভাইয়া" (bhaiya), "আপু" (apu), "দাদা", "দোস্ত", etc.!
   - Always use formal Bengali honorifics like "আপনি", "আপনার", "করবেন", "জানাবেন", etc.
${trackUrl || hotline ? `6. ORDER TRACKING & SUPPORT:
   - ${trackUrl ? `Tracking link: ${trackUrl}` : ''} ${hotline ? `| Hotline: ${hotline}` : ''}` : ''}`;
                } else if (customApi) {
                    if (customApi.instructions) {
                        agentInstructions += `\n\n--- BUSINESS & CUSTOM API GUIDELINES ---\n${customApi.instructions}\n`;
                    }
                    if (customApi.action_endpoint) {
                        const actName = customApi.action_name || 'custom_action';
                        agentInstructions += `
- "action": (string) If the user requested an action/operation and all required details are gathered, return "${actName}". Otherwise return null.
- "action_payload": (object or null) Parameters/data to send to the action endpoint.`;
                    } else {
                        agentInstructions += `\n- "action": (string) Always null.`;
                    }
                } else {
                    agentInstructions += `\n- "action": (string) Always null.`;
                }

                if (chatbot.enable_human_handoff) {
                    agentInstructions += `\n- "needs_handoff": (boolean) Set to true ONLY if the user is frustrated, angry, explicitly asks for a human agent, or if you cannot resolve their issue. Otherwise false.`;
                } else {
                    agentInstructions += `\n- "needs_handoff": (boolean) Always false.`;
                }

                if (chatbot.enable_google_meet) {
                    agentInstructions += `\n- If the user explicitly asks for a meeting or call, action can be "generate_google_meet".`;
                }

                const fullPrompt = `${chatbot.system_prompt || ''}${liveContext ? `\n\n--- REAL-TIME STORE CONTEXT ---\n${liveContext}\n` : ''}${recentHistoryText ? `\n\n--- RECENT CONVERSATION HISTORY ---\n${recentHistoryText}\n` : ''}${agentInstructions}\n\nCustomer: ${incomingText}`;

                let aiResponseText = await callAIModel(userId, chatbot.ai_model, chatbot.api_key, fullPrompt);
                let aiResponseObj;
                
                try {
                    const cleanJsonStr = aiResponseText.replace(/^```json/im, '').replace(/^```/im, '').replace(/```$/m, '').trim();
                    aiResponseObj = JSON.parse(cleanJsonStr);
                } catch (e) {
                    console.warn('[Chatbot] Failed to parse AI JSON, falling back to raw text');
                    aiResponseObj = { reply_text: aiResponseText, needs_handoff: false, action: null };
                }

                if (chatbot.enable_human_handoff && aiResponseObj.needs_handoff) {
                    console.log(`[Chatbot] Handoff triggered for contact ${contactDoc._id}`);
                    contactDoc.chatbot_paused = true;
                    contactDoc.assigned_chatbot = null;
                    await contactDoc.save();

                    const io = unifiedWhatsAppService.io;
                    if (io) {
                        io.emit('agent:escalation', {
                            contact_id: contactDoc._id.toString(),
                            workspace_id: contactDoc.workspace_id?.toString() || null,
                            user_id: userId,
                            message: "AI Agent requested human handoff",
                            timestamp: new Date().toISOString()
                        });
                    }
                }

                let finalReplyText = aiResponseObj.reply_text || "I'm having trouble processing that right now.";

                if (chatbot.enable_google_meet && aiResponseObj.action === 'generate_google_meet') {
                    try {
                        const { default: GoogleAccount } = await import('../models/google-account.model.js');
                        const { getCalendarClient } = await import('./google-api-helper.js');
                        
                        const googleAccount = await GoogleAccount.findOne({ user_id: userId, status: 'active', deleted_at: null });
                        if (googleAccount) {
                            const calendarClient = await getCalendarClient(googleAccount._id);
                            const start = new Date();
                            const end = new Date(start.getTime() + 60 * 60 * 1000);
                            const event = await calendarClient.events.insert({
                                calendarId: 'primary',
                                conferenceDataVersion: 1,
                                requestBody: {
                                    summary: `Meeting with ${contactDoc.name || 'Client'}`,
                                    start: { dateTime: start.toISOString() },
                                    end: { dateTime: end.toISOString() },
                                    conferenceData: { createRequest: { requestId: `meet_${Date.now()}` } }
                                }
                            });
                            const meetLink = event.data.hangoutLink;
                            if (meetLink) {
                                finalReplyText += `\n\nHere is the Google Meet link for our meeting: ${meetLink}`;
                            }
                        }
                    } catch (meetErr) {
                        console.error('[Chatbot] Error generating Google Meet link:', meetErr);
                    }
                }

                if (ecommerce && aiResponseObj.action === 'place_order' && aiResponseObj.order_data) {
                    try {
                        const od = aiResponseObj.order_data;
                        const contextHasProductId = liveContext && od.product_id && (
                            liveContext.includes(`Product ID: ${od.product_id}`) ||
                            liveContext.includes(`[ID: ${od.product_id}]`)
                        );

                        if (!contextHasProductId) {
                            console.warn(`[Ecommerce Chatbot] BLOCKED unverified product_id: ${od.product_id}. Live context did not verify this product!`);
                            finalReplyText = finalReplyText.replace(/🎉[\s\S]*$/m, '').trim();
                            if (!finalReplyText.includes('লিংক') && !finalReplyText.includes('link')) {
                                finalReplyText += `\n\nদুঃখিত, পণ্যটির সঠিক তথ্য আমাদের স্টোর সিস্টেমে সরাসরি কনফার্ম করা সম্ভব হয়নি। অনুগ্রহ করে পণ্যটির সঠিক লিংক বা নাম শেয়ার করুন অথবা আমাদের হটলাইনে (${ecommerce.support_hotline || 'যোগাযোগ করুন'}) জানান।`;
                            }
                        } else {
                            console.log('[Ecommerce Chatbot] Placing verified automatic order:', od);

                            // Load live product details to accurately resolve variation and attributes
                            let pDetail = null;
                            try {
                                pDetail = await getEcommerceLiveProduct(ecommerce, od.product_id);
                            } catch (e) {
                                console.warn('[Ecommerce Chatbot] Could not fetch product detail for variation resolution:', e.message);
                            }

                            // If product has variations, resolve matching variation ID and snapshot
                            const resolvedVar = resolveProductVariation(
                                pDetail,
                                `${incomingText} ${recentHistoryText}`,
                                od
                            );

                            // Check if product requires variation selection but none was specified
                            let pVariations = pDetail?.variations;
                            if (typeof pVariations === 'string') {
                                try { pVariations = JSON.parse(pVariations); } catch (e) { pVariations = []; }
                            }
                            const hasVariations = Array.isArray(pVariations) && pVariations.length > 0;

                            if (hasVariations && !resolvedVar.variation_id && !resolvedVar.variation_snapshot) {
                                console.warn(`[Ecommerce Chatbot] Product ${od.product_id} has variations, but customer has not specified color/size yet.`);
                                finalReplyText = finalReplyText.replace(/🎉[\s\S]*$/m, '').trim();
                                const varList = pVariations.slice(0, 8).map(v => {
                                    const c = v.color ? `কালার: ${v.color}` : '';
                                    const s = (v.attributes?.Size || v.size) ? `সাইজ: ${v.attributes?.Size || v.size}` : '';
                                    return [c, s].filter(Boolean).join(', ');
                                }).filter(Boolean);
                                const uniqueVars = [...new Set(varList)].join(' | ');

                                finalReplyText += `\n\nঅনুগ্রহ করে আপনার পছন্দের সাইজ বা কালারটি জানান (উপলব্ধ: ${uniqueVars || 'উপরে দেওয়া অপশনগুলো'}) যাতে আমরা আপনার সঠিক পণ্যটি কনফার্ম করতে পারি।`;
                            } else {
                                const productItem = {
                                    product_id: od.product_id,
                                    quantity: od.quantity || 1,
                                    price: od.price
                                };

                                if (resolvedVar.variation_id) {
                                    productItem.product_variation_id = resolvedVar.variation_id;
                                    productItem.variant_id = resolvedVar.variation_id;
                                }
                                if (resolvedVar.variation_snapshot) {
                                    productItem.variation_snapshot = resolvedVar.variation_snapshot;
                                }

                                const orderRes = await placeEcommerceOrder(ecommerce, {
                                    name: od.name || contactDoc.name || 'Customer',
                                    phone: od.phone || contactDoc.phone,
                                    address: od.address,
                                    shipping_cost: od.shipping_cost || ecommerce.shipping_inside_dhaka || 80,
                                    products: [productItem],
                                    notes: `Ordered via WhatsApp AI Chatbot${resolvedVar.variation_snapshot ? ` [${resolvedVar.variation_snapshot}]` : ''}`
                                });

                                if (orderRes.success && orderRes.order) {
                                    const orderNum = orderRes.order.order_number || orderRes.order.id;
                                    const varNote = resolvedVar.variation_snapshot ? `\nভ্যারিয়েশন: ${resolvedVar.variation_snapshot}` : '';
                                    finalReplyText += `\n\n🎉 আপনার অর্ডারটি সফলভাবে গ্রহণ করা হয়েছে! \nঅর্ডার নম্বর: #${orderNum}${varNote} ${ecommerce.order_track_url ? `\nট্র্যাকিং লিংক: ${ecommerce.order_track_url}` : ''}`;
                                }
                            }
                        }
                    } catch (orderErr) {
                        console.error('[Ecommerce Chatbot] Error executing placeEcommerceOrder:', orderErr);
                    }
                }

                if (customApi && customApi.action_endpoint && aiResponseObj.action === (customApi.action_name || 'custom_action') && aiResponseObj.action_payload) {
                    try {
                        console.log(`[Custom API Chatbot] Executing action ${customApi.action_name || 'custom_action'}:`, aiResponseObj.action_payload);
                        const actionRes = await executeCustomApiAction(customApi, {
                            ...aiResponseObj.action_payload,
                            contact_id: contactDoc._id,
                            contact_name: contactDoc.name,
                            contact_phone: contactDoc.phone
                        });
                        if (actionRes.success) {
                            console.log('[Custom API Chatbot] Action executed successfully:', actionRes.data);
                        }
                    } catch (customActionErr) {
                        console.error('[Custom API Chatbot] Action error:', customActionErr);
                    }
                }

                // Clean any parentheses around links, dangling closing parens, ensure list formatting and auto-correct product URLs/slugs
                finalReplyText = cleanReplyUrlsAndFormatting(finalReplyText, activeMatchedProduct);

                let messageTypeToSend = 'text';
                let messageFile = null;

                if (chatbot.voice_settings && chatbot.voice_settings.enabled) {
                    const voiceProvider = chatbot.voice_settings.provider || null;

                    const { generateSpeech } = await import('./voice-ai.service.js');
                    console.log(`[Chatbot] Generating voice response via TTS...`);
                    const ttsResult = await generateSpeech(finalReplyText, chatbot.voice_settings.voice_id, chatbot.api_key, chatbot.ai_model, voiceProvider);
                    if (ttsResult && ttsResult.buffer) {
                        messageTypeToSend = 'audio';
                        messageFile = {
                            buffer: ttsResult.buffer,
                            mimetype: 'audio/mpeg',
                            originalname: ttsResult.fileName || 'audio_message.mp3'
                        };
                    }
                }

                if (isOmnichannel) {
                   await sendOmnichannelMessageHelper({
                        contactDoc,
                        messageType: 'text',
                        text: finalReplyText
                    });
                } else {
                    await unifiedWhatsAppService.sendMessage(userId, {
                        recipientNumber: params.senderNumber,
                        messageText: finalReplyText,
                        messageType: messageTypeToSend,
                        file: messageFile,
                        whatsappPhoneNumberId
                    });
                }

                contactDoc.ai_message_count = (contactDoc.ai_message_count || 0) + 1;
                await contactDoc.save();

                return;
            }
        }

        if (replyType === 'agent' || replyType === 'assign_agent') {
            await assignToAgentInternal({ contactId, agentId: replyId, whatsappPhoneNumberId, adminId: userId });
            if (contactDoc) {
                contactDoc.assigned_to = replyId;
                await contactDoc.save();
            }
            return;
        }

        if (replyType === 'sequence' || replyType === 'Sequence') {
            await handleSequenceReply({ wabaId, contactId, sequenceId: replyId, userId, whatsappPhoneNumberId });
            return;
        }

        if (replyType === 'appointment_flow') {
            const { AppointmentConfig } = await import('../models/index.js');
            const { default: appointmentService } = await import('../services/appointment.service.js');

            let configId = replyId;
            const material = await ReplyMaterial.findById(replyId).lean();
            if (material && material.appointment_config_id) {
                configId = material.appointment_config_id;
            }

            const config = await AppointmentConfig.findById(configId).lean();
            if (config) {
                await appointmentService.startConversationalFlow({
                    userId,
                    contactId,
                    configId: config._id,
                    whatsappPhoneNumberId,
                    inputData: { senderNumber: params.senderNumber, whatsappPhoneNumberId }
                });
                return;
            }
        }

        const isTemplate = replyType === 'template' || replyType === 'Template';

        let automationData = typeof sequenceAutomationData !== 'undefined' && sequenceAutomationData ? sequenceAutomationData : {};
        if (automationData) {
            if (automationData.variables_mapping) automationData.templateVariables = automationData.variables_mapping;
            if (automationData.media_url) automationData.mediaUrl = automationData.media_url;
            if (automationData.carousel_cards_data) automationData.carouselCardsData = automationData.carousel_cards_data;
            if (automationData.coupon_code) automationData.couponCode = automationData.coupon_code;
            if (automationData.catalog_id) automationData.catalogId = automationData.catalog_id;
            if (automationData.product_retailer_id) automationData.productRetailerId = automationData.product_retailer_id;
        }

        if (replyType === 'chatbot') {
        } else if (replyType === 'agent' || replyType === 'assign_agent') {
        } else if (replyType === 'sequence' || replyType === 'Sequence') {
        } else if (!automationData.templateVariables) {
            const bot = activeBot || await MessageBot.findOne({ account_id: params.senderNumber || wabaId, reply_id: replyId, status: 'active' }).lean();
            if (bot) {
                automationData = {
                    templateVariables: bot.variables_mapping,
                    mediaUrl: bot.media_url,
                    carouselCardsData: bot.carousel_cards_data,
                    couponCode: bot.coupon_code,
                    catalogId: bot.catalog_id,
                    productRetailerId: bot.product_retailer_id
                };
            } else {
                const config = await WabaConfiguration.findOne({ waba_id: wabaId }).lean();
                if (config) {
                    const fields = ['welcome_message', 'out_of_working_hours', 'delayed_reply', 'fallback_message', 'reengagement_message'];
                    for (const field of fields) {
                        const item = config[field];
                        if (item && item.id && item.id.toString() === replyId.toString()) {
                            automationData = {
                                templateVariables: item.variables_mapping,
                                mediaUrl: item.media_url,
                                carouselCardsData: item.carousel_cards_data,
                                couponCode: item.coupon_code,
                                catalogId: item.catalog_id,
                                productRetailerId: item.product_retailer_id
                            };
                            break;
                        }
                    }
                }
            }
        }

        if (automationData.templateVariables) {
            const rawVars = automationData.templateVariables instanceof Map ? Object.fromEntries(automationData.templateVariables) : automationData.templateVariables;
            const resolvedVars = {};
            for (const [key, value] of Object.entries(rawVars)) {
                if (value === undefined || value === null) continue;
                const strVal = String(value).trim();
                const match = strVal.match(/^\{\{([a-zA-Z_][a-zA-Z0-9_]*)\}\}$/);

                let resolved = strVal;
                if (match) {
                    const fieldName = match[1];
                    resolved = '';
                    if (contactDoc) {
                        if (fieldName in contactDoc && contactDoc[fieldName] != null) {
                            resolved = String(contactDoc[fieldName]);
                        } else if (contactDoc.custom_fields) {
                            const cf = contactDoc.custom_fields;
                            const val = typeof cf.get === 'function' ? cf.get(fieldName) : cf[fieldName];
                            if (val != null) resolved = String(val);
                        }
                    }
                    if (!resolved || resolved.trim() === '') {
                        const lowerField = fieldName.toLowerCase();
                        resolved = lowerField.includes('name') ? 'Customer' : 'N/A';
                    }
                }
                resolvedVars[key] = resolved;
            }
            automationData.templateVariables = resolvedVars;
        }

        if (isOmnichannel) {
            let msgType = 'text';
            let msgText = '';
            let fileUrl = '';
            let isTemplate = false;
            let templateObj = null;
            let buttons = undefined;

            if (replyType === 'ReplyMaterial' || replyType === 'replymaterial' || ['text', 'media', 'image', 'video', 'audio', 'document'].includes(replyType)) {
                const material = await ReplyMaterial.findById(replyId).lean();
                if (material) {
                    msgType = material.type;
                    if (material.type === 'text') {
                        msgText = material.content;
                    } else {
                        fileUrl = material.file_path;
                        msgText = material.content || '';
                    }
                }
            } else if (replyType === 'template' || replyType === 'Template') {
                const template = await Template.findById(replyId).lean();
                if (template) {
                    isTemplate = true;
                    templateObj = template;
                    msgType = 'text';
                    msgText = template.message_body || '';

                    const vars = automationData?.templateVariables || {};
                    if (vars) {
                        for (const [key, value] of Object.entries(vars)) {
                            const regex = new RegExp(`\\{\\{${key}\\}\\}`, 'g');
                            msgText = msgText.replace(regex, value);
                        }
                    }

                    if (template.header && (
                        ['IMAGE', 'VIDEO', 'DOCUMENT'].includes((template.header.format || '').toUpperCase()) ||
                        template.header.format === 'media'
                    )) {
                        fileUrl = automationData?.mediaUrl || template.header.media_url;
                        msgType = (template.header.media_type || template.header.format || 'image').toLowerCase();
                    }

                    if (template.buttons && template.buttons.length > 0) {
                        buttons = template.buttons;
                    }
                }
            }

            await sendOmnichannelMessageHelper({
                contactDoc,
                messageType: msgType,
                text: msgText,
                fileUrl,
                buttons,
                isTemplate,
                templateId: replyId,
                templateObj
            });
            return;
        }

        await unifiedWhatsAppService.sendMessage(userId, {
            recipientNumber: params.senderNumber,
            whatsappPhoneNumberId,
            contactId,
            replyType,
            replyId,
            ...automationData
        });

    } catch (error) {
        console.error('Error sending automated reply:', error);
    }
};

const assignToAgentInternal = async ({ contactId, agentId, whatsappPhoneNumberId, adminId }) => {
    try {
        const contact = await Contact.findById(contactId);
        const phoneNumber = await WhatsappPhoneNumber.findById(whatsappPhoneNumberId);

        if (!contact || !phoneNumber) {
            console.error('Contact or WhatsApp phone number not found in assignToAgentInternal');
            return;
        }

        const contactPhoneNumber = contact.phone_number;
        const businessPhoneNumber = phoneNumber.display_phone_number;

        const chatMatch = {
            $or: [
                { sender_number: contactPhoneNumber, receiver_number: businessPhoneNumber },
                { sender_number: businessPhoneNumber, receiver_number: contactPhoneNumber }
            ]
        };

        const existingAssignment = await ChatAssignment.findOne({
            whatsapp_phone_number_id: whatsappPhoneNumberId,
            ...chatMatch
        });

        if (existingAssignment) {
            existingAssignment.agent_id = agentId;
            existingAssignment.status = 'assigned';
            existingAssignment.assigned_by = adminId;
            existingAssignment.updated_at = new Date();
            await existingAssignment.save();
        } else {
            await ChatAssignment.create({
                sender_number: contactPhoneNumber,
                receiver_number: businessPhoneNumber,
                whatsapp_phone_number_id: whatsappPhoneNumberId,
                agent_id: agentId,
                assigned_by: adminId,
                status: 'assigned'
            });
        }
    } catch (error) {
        console.error('Error in assignToAgentInternal:', error);
    }
};

export const handleSequenceReply = async ({ wabaId, contactId, sequenceId, userId, whatsappPhoneNumberId, params }) => {
    try {
        console.log("calledd");
        const { Sequence, SequenceStep, Contact } = db;
        const sequence = await Sequence.findById(sequenceId);
        if (!sequence || !sequence.is_active) return;

        const steps = await SequenceStep.find({ sequence_id: sequenceId, deleted_at: null }).sort({ sort: 1 });
        if (steps.length === 0) return;

        console.log(`Assigning sequence ${sequence.name} to contact ${contactId}`);

        let cumulativeDelayMs = 0;

        for (const step of steps) {
            if (!step.is_active) continue;

            const { getSequenceQueue } = await import('../queues/sequence-queue.js');
            const sequenceQueue = await getSequenceQueue();

            let delayMs = 0;
            if (step.delay_value > 0) {
                switch (step.delay_unit) {
                    case 'minutes': delayMs += step.delay_value * 60 * 1000; break;
                    case 'hours': delayMs += step.delay_value * 60 * 60 * 1000; break;
                    case 'days': delayMs += step.delay_value * 24 * 60 * 60 * 1000; break;
                }
            }

            cumulativeDelayMs += delayMs;

            await sequenceQueue.add('send_sequence_step', {
                wabaId,
                contactId,
                step,
                userId,
                whatsappPhoneNumberId,
                params
            }, {
                delay: cumulativeDelayMs > 0 ? cumulativeDelayMs : undefined,
                jobId: `seq_${sequenceId}_${contactId}_step_${step._id}_${Date.now()}`
            });

            console.log(`Enqueued sequence step ${step.sort} in ${cumulativeDelayMs} ms`);
        }

    } catch (error) {
        console.error('Error in handleSequenceReply:', error);
    }
};

export const canSendSequenceStep = async (contactId, materialType) => {
    return true;
};

export const assignRoundRobin = async (userId, contactId, whatsappPhoneNumberId) => {
    try {
        console.log("called")
        const agents = await User.find({ created_by: userId, role: 'agent', status: 'active' }).sort({ _id: 1 });
        console.log("agents", agents);
        if (agents.length === 0) return null;

        const lastAssignment = await ChatAssignment.findOne({
            whatsapp_phone_number_id: whatsappPhoneNumberId
        }).sort({ createdAt: -1 });

        let nextAgentIndex = 0;
        if (lastAssignment && lastAssignment.agent_id) {
            const lastAgentIndex = agents.findIndex(a => a._id.toString() === lastAssignment.agent_id.toString());
            if (lastAgentIndex !== -1) {
                nextAgentIndex = (lastAgentIndex + 1) % agents.length;
            }
        }

        const selectedAgent = agents[nextAgentIndex];
        await assignToAgentInternal({ contactId, agentId: selectedAgent._id, whatsappPhoneNumberId, adminId: userId });
        return selectedAgent;
    } catch (error) {
        console.error('Error in assignRoundRobin:', error);
        return null;
    }
};


export const processAutomatedPipeline = async ({
    workspaceId,
    contactDoc,
    incomingText,
    channel,
    accountId,
    userId,
    whatsappPhoneNumberId,
    businessAccountId
}) => {
    try {
        let automatedHandled = false;

        let wabaId = null;
        let config = null;
        let connectionId = null;

        if (channel === 'whatsapp' || channel === 'baileys') {
            const waba = await WhatsappWaba.findOne({ workspace_id: workspaceId, deleted_at: null });
            if (!waba) {
                console.warn(`[Automated Pipeline] No WhatsappWaba found for workspace: ${workspaceId}`);
            }

            wabaId = waba?._id || null;
            connectionId = wabaId;
            config = wabaId ? await WabaConfiguration.findOne({ waba_id: wabaId }).lean() : null;

            if (wabaId) {
                const withinWorkingHours = await isWithinWorkingHours(wabaId);
                if (!withinWorkingHours && config?.out_of_working_hours?.id) {
                    await sendAutomatedReply({
                        wabaId,
                        contactId: contactDoc._id,
                        replyType: config.out_of_working_hours.type,
                        replyId: config.out_of_working_hours.id,
                        senderNumber: accountId,
                        incomingText,
                        userId,
                        whatsappPhoneNumberId
                    });
                    automatedHandled = true;
                }
            }
        } else {
            const waba = await WhatsappWaba.findOne({ workspace_id: workspaceId, deleted_at: null });
            if (waba) {
                wabaId = waba._id;
            }

            if (channel === 'telegram') {
                const bot = await db.TelegramConnection.findOne({ workspace_id: workspaceId, is_active: true }).lean();
                if (bot) connectionId = bot._id;
            } else if (channel === 'facebook') {
                const page = await db.FacebookConnection.findOne({ workspace_id: workspaceId, is_active: true }).lean();
                if (page) connectionId = page._id;
            } else if (channel === 'instagram') {
                const ig = await db.InstagramConnection.findOne({ workspace_id: workspaceId, is_active: true }).lean();
                if (ig) connectionId = ig._id;
            }
        }

        if (!automatedHandled && connectionId) {
            let matchingBot = await findMatchingBot(connectionId, incomingText, channel, contactDoc);

            if (!matchingBot && wabaId) {
                matchingBot = await findMatchingBot(wabaId, incomingText, channel, contactDoc);
            }

            if (!matchingBot && workspaceId) {
                matchingBot = await findMatchingBot(workspaceId, incomingText, channel, contactDoc);
            }

            if (matchingBot) {
                await sendAutomatedReply({
                    wabaId,
                    contactId: contactDoc._id,
                    replyType: matchingBot.reply_type,
                    replyId: matchingBot.reply_id,
                    senderNumber: accountId,
                    incomingText,
                    userId,
                    whatsappPhoneNumberId,
                    botId: matchingBot._id,
                    bot: matchingBot
                });
                automatedHandled = true;
            }
        }

        const isNewContact = (Date.now() - new Date(contactDoc.created_at).getTime() < 10000);

        if (channel === 'whatsapp' || channel === 'baileys') {
            if (!automatedHandled && isNewContact && config?.welcome_message?.id) {
                await sendAutomatedReply({
                    wabaId,
                    contactId: contactDoc._id,
                    replyType: config.welcome_message.type,
                    replyId: config.welcome_message.id,
                    senderNumber: accountId,
                    incomingText,
                    userId,
                    whatsappPhoneNumberId
                });
                automatedHandled = true;

                if (config?.round_robin_assignment) {
                    await assignRoundRobin(userId, contactDoc._id, whatsappPhoneNumberId);
                }
            }

            if (!automatedHandled && config?.fallback_message?.id && wabaId) {
                await sendAutomatedReply({
                    wabaId,
                    contactId: contactDoc._id,
                    replyType: config.fallback_message.type,
                    replyId: config.fallback_message.id,
                    senderNumber: accountId,
                    incomingText,
                    userId,
                    whatsappPhoneNumberId
                });
                automatedHandled = true;
            }
        }

        return automatedHandled;
    } catch (error) {
        console.error('[Automated Pipeline] Error in processAutomatedPipeline:', error);
        return false;
    }
};
