import mongoose from 'mongoose';
import path from 'path';
import Campaign from '../models/campaign.model.js';
import Template from '../models/template.model.js';
import Contact from '../models/contact.model.js';
import WhatsappWaba from '../models/whatsapp-waba.model.js';
import * as segmentService from '../services/segment.service.js';
import { processCampaignInBackground } from '../utils/campaign-processing.js';
import { saveBufferLocally } from '../utils/whatsapp-message-handler.js';
import { getWhatsAppTypeFromMime } from '../utils/uploadMediaToWhatsapp.js';
import AIModel from '../models/ai-model.model.js';
import UserSetting from '../models/user-setting.model.js';

const API_VERSION = 'v23.0';

const DEFAULT_PAGE = 1;
const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 100;

const parsePaginationParams = (query) => {
  const page = Math.max(1, parseInt(query.page) || DEFAULT_PAGE);
  const limit = Math.max(1, Math.min(MAX_LIMIT, parseInt(query.limit) || DEFAULT_LIMIT));
  const skip = (page - 1) * limit;

  return { page, limit, skip };
};

const SORT_ORDER = {
  ASC: 1,
  DESC: -1
};

const DEFAULT_SORT_FIELD = 'created_at';
const ALLOWED_SORT_FIELDS = ['name', 'description', 'recipient_type', 'status', 'created_at', 'sent_at'];

const parseSortParams = (query) => {
  const sortField = ALLOWED_SORT_FIELDS.includes(query.sort_by)
    ? query.sort_by
    : DEFAULT_SORT_FIELD;

  const sortOrder = query.sort_order?.toUpperCase() === 'DESC'
    ? SORT_ORDER.DESC
    : SORT_ORDER.ASC;

  return { sortField, sortOrder };
};

const sanitizeWorkspaceId = (workspaceId) => {
  if (workspaceId === 'null' || workspaceId === 'undefined' || !workspaceId || !mongoose.Types.ObjectId.isValid(workspaceId)) {
    return null;
  }
  return new mongoose.Types.ObjectId(workspaceId);
};

export const createCampaign = async (req, res) => {
  try {
    let {
      name,
      description,
      waba_id,
      template_id,
      template_name,
      language_code,
      recipient_type,
      specific_contacts = [],
      contact_numbers = [],
      tag_ids = [],
      segment_ids = [],
      variables_mapping = {},
      media_url,
      coupon_code,
      location_data,
      carousel_products,
      carousel_cards_data,
      offer_expiration_minutes,
      is_scheduled = false,
      scheduled_at,
      is_published = false,
      avoid_unsubscribers = true,
      platform = 'whatsapp',
      workspace_id,
      is_recurring = false,
      recurring_pattern,
      cron_expression,
      recurring_end_date,
      batch_size,
      pause_between_batches
    } = req.body;

    const userId = req.user.owner_id;
    const resolvedWorkspaceId = sanitizeWorkspaceId(workspace_id || req.query.workspace_id || req.headers['x-workspace-id']);

    if (!name) {
      return res.status(400).json({ error: 'Campaign name is required' });
    }

    if (platform === 'whatsapp' && !waba_id) {
      return res.status(400).json({ error: 'WABA ID is required' });
    }

    if (!template_id && !template_name) {
      return res.status(400).json({ error: 'Template ID or Template Name is required' });
    }

    if (!recipient_type) {
      return res.status(400).json({ error: 'Recipient type is required' });
    }

    if (recipient_type === 'specific_contacts' && (!specific_contacts || specific_contacts.length === 0) && (!contact_numbers || contact_numbers.length === 0)) {
      return res.status(400).json({ error: 'Specific contacts or contact numbers are required for this recipient type' });
    }

    if (recipient_type === 'tags' && (!tag_ids || tag_ids.length === 0)) {
      return res.status(400).json({ error: 'Tag IDs are required for this recipient type' });
    }

    if (recipient_type === 'segments' && (!segment_ids || segment_ids.length === 0)) {
      return res.status(400).json({ error: 'Segment IDs are required for this recipient type' });
    }

    const isScheduledBool =
      is_scheduled === true ||
      is_scheduled === 'true' ||
      is_scheduled === 1 ||
      is_scheduled === '1';

    const isPublishedBool =
      is_published === true ||
      is_published === 'true' ||
      is_published === 1 ||
      is_published === '1';

    if (isScheduledBool && !scheduled_at) {
      return res.status(400).json({
        error: 'Scheduled time is required for scheduled campaigns'
      });
    }

    let parsedScheduledAt = null;
    if (isScheduledBool && scheduled_at) {
      parsedScheduledAt = new Date(scheduled_at);
      if (isNaN(parsedScheduledAt.getTime())) {
        return res.status(400).json({
          error: 'Invalid scheduled time format'
        });
      }
      if (isPublishedBool && parsedScheduledAt < new Date()) {
        return res.status(400).json({
          error: 'Scheduled time has passed. Do you want to send immediately or reschedule?'
        });
      }
    }

    const isRecurringBool =
      is_recurring === true ||
      is_recurring === 'true' ||
      is_recurring === 1 ||
      is_recurring === '1';

    if (isRecurringBool) {
      if (!isScheduledBool) {
        return res.status(400).json({ error: 'Recurring campaigns must have a scheduled start time.' });
      }
      if (!recurring_pattern) {
        return res.status(400).json({ error: 'recurring_pattern is required for recurring campaigns' });
      }
      if (recurring_pattern === 'custom_cron' && !cron_expression) {
        return res.status(400).json({ error: 'cron_expression is required for custom_cron pattern' });
      }
    }

    let parsedRecurringEndDate = null;
    if (isRecurringBool && recurring_end_date) {
      parsedRecurringEndDate = new Date(recurring_end_date);
      if (isNaN(parsedRecurringEndDate.getTime())) {
        return res.status(400).json({ error: 'Invalid recurring end date format' });
      }
    }

    let waba_id_obj = null;
    if (platform === 'whatsapp') {
      let wabaQuery = {
        user_id: userId,
        deleted_at: null,
      };
      if (resolvedWorkspaceId) {
        wabaQuery.workspace_id = resolvedWorkspaceId;
      }

      if (mongoose.Types.ObjectId.isValid(waba_id)) {
        wabaQuery.$or = [
          { _id: waba_id },
          { whatsapp_business_account_id: waba_id }
        ];
      } else {
        wabaQuery.whatsapp_business_account_id = waba_id;
      }

      const waba = await WhatsappWaba.findOne(wabaQuery);

      if (!waba) {
        return res.status(404).json({ error: 'WhatsApp WABA not found' });
      }

      waba_id_obj = waba._id.toString();
    }

    let templateQuery = {
      user_id: userId,
      deleted_at: null
    };

    let andConditions = [];

    if (resolvedWorkspaceId) {
      andConditions.push({
        $or: [
          { workspace_id: resolvedWorkspaceId },
          { workspace_id: null },
          { workspace_id: { $exists: false } }
        ]
      });
    }

    if (platform === 'whatsapp') {
      andConditions.push({
        $or: [
          { platform: 'whatsapp' },
          { platform: { $exists: false } },
          { platform: null }
        ]
      });
    } else {
      templateQuery.platform = platform;
    }

    if (andConditions.length > 0) {
      templateQuery.$and = andConditions;
    }

    if (template_name) {
      templateQuery.template_name = template_name.toLowerCase();
    } else if (template_id) {
      templateQuery._id = template_id;
    }

    const template = await Template.findOne(templateQuery);

    if (!template) {
      return res.status(404).json({ error: 'Template not found' });
    }

    template_id = template._id.toString();

    const templateType = (template.template_type || '').toLowerCase();
    const isCarouselTemplate = ['carousel_product', 'carousel_media'].includes(templateType);
    const carouselProducts = typeof req.body.carousel_products === 'string' ? JSON.parse(req.body.carousel_products) : req.body.carousel_products;
    const carouselCardsData = typeof req.body.carousel_cards_data === 'string' ? JSON.parse(req.body.carousel_cards_data) : req.body.carousel_cards_data;

    specific_contacts = typeof specific_contacts === 'string' ? JSON.parse(specific_contacts) : specific_contacts;
    contact_numbers = typeof contact_numbers === 'string' ? JSON.parse(contact_numbers) : contact_numbers;
    tag_ids = typeof tag_ids === 'string' ? JSON.parse(tag_ids) : tag_ids;
    segment_ids = typeof segment_ids === 'string' ? JSON.parse(segment_ids) : segment_ids;


    const isProductCarousel = isCarouselTemplate && template.carousel_cards?.length > 0 &&
      template.carousel_cards[0].components?.some(c => (c.type || '').toLowerCase() === 'header' && (c.format || '').toLowerCase() === 'product');

    if (isCarouselTemplate && platform === 'whatsapp') {
      if (isProductCarousel) {
        if (!carouselProducts || !Array.isArray(carouselProducts) || carouselProducts.length === 0) {
          return res.status(400).json({
            error: 'Product carousel template requires carousel_products: array of { product_retailer_id, catalog_id }'
          });
        }
        if (carouselProducts.length > 10) {
          return res.status(400).json({ error: 'Carousel supports at most 10 cards' });
        }
      } else {
        if (!carouselCardsData || !Array.isArray(carouselCardsData) || carouselCardsData.length === 0) {
          return res.status(400).json({
            error: 'Media carousel template requires carousel_cards_data: array of { header: { type, id or link }, buttons: [{ type, payload or url_value }] }'
          });
        }
        if (carouselCardsData.length > 10) {
          return res.status(400).json({ error: 'Carousel supports at most 10 cards' });
        }
      }
    }

    let avoidUnsub = avoid_unsubscribers;
    if (typeof avoidUnsub === 'string') avoidUnsub = avoidUnsub === 'true';

    const platformFilter = {};
    if (platform === 'whatsapp') {
      platformFilter.phone_number = { $exists: true, $ne: '' };
    } else if (platform === 'telegram') {
      platformFilter.telegram_chat_id = { $exists: true, $ne: '' };
    } else if (platform === 'facebook') {
      platformFilter.facebook_page_scoped_id = { $exists: true, $ne: '' };
      platformFilter.last_incoming_message_at = { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) };
    } else if (platform === 'instagram') {
      platformFilter.instagram_scoped_id = { $exists: true, $ne: '' };
      platformFilter.last_incoming_message_at = { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) };
    }

    let contacts = [];
    if (recipient_type === 'all_contacts') {
      const allContactsQuery = {
        user_id: userId,
        deleted_at: null,
        ...platformFilter
      };
      if (resolvedWorkspaceId) {
        allContactsQuery.$or = [
          { workspace_id: resolvedWorkspaceId },
          { workspace_id: null },
          { workspace_id: { $exists: false } }
        ];
      }
      contacts = await Contact.find(allContactsQuery);
    } else if (recipient_type === 'specific_contacts') {
      if (specific_contacts && specific_contacts.length > 0) {
        const specificQuery = {
          _id: { $in: specific_contacts },
          user_id: userId,
          deleted_at: null,
          ...platformFilter
        };
        contacts = await Contact.find(specificQuery);
      } else if (contact_numbers && contact_numbers.length > 0) {
        const cleanedNumbers = contact_numbers.map(num => num.replace(/[\s\-()\+]/g, ''));
        const invalidNumbers = cleanedNumbers.filter(num => !/^\d{6,15}$/.test(num));

        if (invalidNumbers.length > 0) {
          return res.status(400).json({
            error: `Invalid contact numbers: ${invalidNumbers.join(', ')}. Must be 6-15 digits.`
          });
        }

        const queryConditions = {
          phone_number: { $in: cleanedNumbers },
          user_id: userId,
          deleted_at: null
        };
        if (platformFilter.phone_number) {
          queryConditions.phone_number = {
            $in: cleanedNumbers,
            $exists: true,
            $ne: ''
          };
        } else {
          Object.assign(queryConditions, platformFilter);
        }

        contacts = await Contact.find(queryConditions);

        const foundNumbers = contacts.map(c => c.phone_number);
        const missingNumbers = cleanedNumbers.filter(num => !foundNumbers.includes(num));

        if (missingNumbers.length > 0) {
          const newContactsToInsert = missingNumbers.map(num => ({
            phone_number: num,
            name: num,
            user_id: userId,
            created_by: userId,
            workspace_id: resolvedWorkspaceId || undefined,
            status: 'lead'
          }));
          const newlyCreated = await Contact.insertMany(newContactsToInsert);
          contacts.push(...newlyCreated);
        }
      }
    } else if (recipient_type === 'tags') {
      const tagsQuery = {
        tags: { $in: tag_ids },
        user_id: userId,
        deleted_at: null,
        ...platformFilter
      };
      if (resolvedWorkspaceId) {
        tagsQuery.$or = [
          { workspace_id: resolvedWorkspaceId },
          { workspace_id: null },
          { workspace_id: { $exists: false } }
        ];
      }
      contacts = await Contact.find(tagsQuery);
    } else if (recipient_type === 'segments') {
      contacts = await segmentService.getContactsForSegments(segment_ids, userId, resolvedWorkspaceId);
      const windowLimit = new Date(Date.now() - 24 * 60 * 60 * 1000);
      if (platform === 'whatsapp') {
        contacts = contacts.filter(c => c.phone_number);
      } else if (platform === 'telegram') {
        contacts = contacts.filter(c => c.telegram_chat_id);
      } else if (platform === 'facebook') {
        contacts = contacts.filter(c => c.facebook_page_scoped_id && c.last_incoming_message_at && new Date(c.last_incoming_message_at) >= windowLimit);
      } else if (platform === 'instagram') {
        contacts = contacts.filter(c => c.instagram_scoped_id && c.last_incoming_message_at && new Date(c.last_incoming_message_at) >= windowLimit);
      }
    }

    const totalFoundBeforeFilter = contacts.length;
    if (avoidUnsub) {
      contacts = contacts.filter(c => c.is_unsubscribed !== true);
    }

    if (contacts.length === 0) {
      if (avoidUnsub && totalFoundBeforeFilter > 0) {
        return res.status(400).json({ error: 'contact are in unsubscribe list you can not send campaign' });
      }
      return res.status(400).json({ error: 'No contacts found for the specified criteria' });
    }

    const recipients = contacts.map(contact => {
      let identifier = contact.phone_number;
      if (platform === 'telegram') {
        identifier = contact.telegram_chat_id || contact.phone_number;
      } else if (platform === 'facebook') {
        identifier = contact.facebook_page_scoped_id || contact.phone_number;
      } else if (platform === 'instagram') {
        identifier = contact.instagram_scoped_id || contact.phone_number;
      }
      return {
        contact_id: contact._id,
        phone_number: identifier,
        status: 'pending'
      };
    });

    const baseUrl = process.env.APP_URL || (req ? `${req.protocol}://${req.get('host')}` : '');
    const uploadedFileUrl = req.file || (req.files && req.files['file_url'] ? req.files['file_url'][0] : null);
    const carouselUploadedFiles = req.files && req.files['carousel_files'] ? req.files['carousel_files'] : [];

    let finalMediaUrl = media_url;
    let originalFilename = null;
    if (uploadedFileUrl) {
      finalMediaUrl = uploadedFileUrl.path.startsWith('http') || uploadedFileUrl.path.startsWith('/') ? uploadedFileUrl.path : `/${uploadedFileUrl.path}`;
      if (!finalMediaUrl.startsWith('http')) {
        finalMediaUrl = `${baseUrl}${finalMediaUrl}`;
      }
      originalFilename = uploadedFileUrl.originalname;
    } else if (media_url) {
      try {
        const decodedPath = decodeURIComponent(media_url);
        const urlWithoutQuery = decodedPath.split('?')[0];
        const extractedFilename = path.basename(urlWithoutQuery);
        if (extractedFilename && extractedFilename.includes('.')) {
          originalFilename = extractedFilename.replace(/-\d{10,13}(?=\.\w+$)/, '');
        }
      } catch (err) {
        // ignore
      }
    }

    let resolvedCarouselCardsData = carouselCardsData;
    if (carouselUploadedFiles.length > 0) {
      const parsed = Array.isArray(resolvedCarouselCardsData) ? resolvedCarouselCardsData : (typeof resolvedCarouselCardsData === 'string' ? JSON.parse(resolvedCarouselCardsData) : []);
      resolvedCarouselCardsData = await Promise.all(parsed.map(async (card, index) => {
        const file = carouselUploadedFiles[index];
        if (file) {
          let fullLink = file.path.startsWith('http') || file.path.startsWith('/') ? file.path : `/${file.path}`;
          if (!fullLink.startsWith('http')) {
            fullLink = `${baseUrl}${fullLink}`;
          }
          return {
            ...card,
            header: {
              type: file.mimetype.startsWith('video/') ? 'video' : 'image',
              link: fullLink
            }
          };
        }
        return card;
      }));
    }

    const campaign = await Campaign.create({
      name,
      description,
      workspace_id: resolvedWorkspaceId || null,
      user_id: userId,
      created_by: req.user.id,
      waba_id: platform === 'whatsapp' ? waba_id_obj : undefined,
      platform,
      template_id,
      template_name: template.template_name,
      language_code: language_code ?? template.language,
      recipient_type,
      specific_contacts: recipient_type === 'specific_contacts' ? specific_contacts : [],
      tag_ids: recipient_type === 'tags' ? tag_ids : [],
      segment_ids: recipient_type === 'segments' ? segment_ids : [],

      variables_mapping: typeof variables_mapping === 'string' ? JSON.parse(variables_mapping) : variables_mapping,
      media_url: finalMediaUrl,
      original_filename: originalFilename,
      coupon_code: coupon_code || null,
      location_data: typeof location_data === 'string' ? JSON.parse(location_data) : (location_data || undefined),
      carousel_products: carouselProducts && Array.isArray(carouselProducts) ? carouselProducts : undefined,
      carousel_cards_data: resolvedCarouselCardsData && Array.isArray(resolvedCarouselCardsData) ? resolvedCarouselCardsData : undefined,
      offer_expiration_minutes: offer_expiration_minutes ?? null,
      is_published: isPublishedBool,
      is_scheduled: isScheduledBool,
      scheduled_at: parsedScheduledAt,
      is_recurring: isRecurringBool,
      recurring_pattern: isRecurringBool ? recurring_pattern : null,
      cron_expression: (isRecurringBool && recurring_pattern === 'custom_cron') ? cron_expression : null,
      recurring_end_date: parsedRecurringEndDate,
      next_run_at: (isRecurringBool && isPublishedBool) ? parsedScheduledAt : null,
      avoid_unsubscribers: avoidUnsub,
      batch_size: batch_size ? parseInt(batch_size, 10) : null,
      pause_between_batches: pause_between_batches ? parseInt(pause_between_batches, 10) : null,
      status: isPublishedBool ? (isRecurringBool ? 'recurring' : (isScheduledBool ? 'scheduled' : 'draft')) : 'draft',
      stats: {
        total_recipients: contacts.length,
        pending_count: contacts.length
      },
      recipients
    });

    if (isPublishedBool && !isScheduledBool) {
      setImmediate(async () => {
        await processCampaignInBackground(campaign._id);
      });

      campaign.status = 'sending';
      campaign.sent_at = new Date();

      return res.status(201).json({
        success: true,
        message: 'Campaign created and sending started',
        data: campaign
      });
    }

    return res.status(201).json({
      success: true,
      message: 'Campaign created successfully',
      data: campaign
    });

  } catch (error) {
    console.error('Error creating campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to create campaign',
      details: error.message
    });
  }
};

export const getAllCampaigns = async (req, res) => {
  try {
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);
    const { page, limit, skip } = parsePaginationParams(req.query);
    const { sortField, sortOrder } = parseSortParams(req.query);
    const { status, search, platform, time_filter } = req.query;

    const matchFilter = {
      user_id: new mongoose.Types.ObjectId(userId),
      deleted_at: null,
      $or: [
        { parent_recurring_id: { $exists: false } },
        { parent_recurring_id: null }
      ]
    };

    if (time_filter) {
      const currentDate = new Date();
      let startDate;

      switch (time_filter) {
        case '7_days_ago':
          startDate = new Date(currentDate.getTime() - 7 * 24 * 60 * 60 * 1000);
          break;
        case '14_days_ago':
          startDate = new Date(currentDate.getTime() - 14 * 24 * 60 * 60 * 1000);
          break;
        case '30_days_ago':
          startDate = new Date(currentDate.getTime() - 30 * 24 * 60 * 60 * 1000);
          break;
        case 'all_time':
        default:
          startDate = null;
      }

      if (startDate) {
        matchFilter.created_at = { $gte: startDate };
      }
    }

    if (workspaceId) {
      matchFilter.workspace_id = workspaceId;
    }

    if (status) {
      matchFilter.status = status;
    }

    if (platform) {
      matchFilter.platform = platform;
    }

    if (search) {
      matchFilter.$or = [
        { name: { $regex: search, $options: 'i' } },
        { description: { $regex: search, $options: 'i' } },
        { status: { $regex: search, $options: 'i' } },
        { recipient_type: { $regex: search, $options: 'i' } },
        { template_name: { $regex: search, $options: 'i' } }
      ];
    }

    const [totalCount, campaigns, campaignStatsAgg] = await Promise.all([
      Campaign.countDocuments(matchFilter),
      Campaign.find(matchFilter)
        .select(
          'name description recipient_type is_published is_scheduled is_recurring scheduled_at sent_at stats status completion_duration_seconds template_id created_at platform'
        )
        .populate({
          path: 'template_id',
          select: 'template_name'
        })
        .sort({ [sortField]: sortOrder })
        .skip(skip)
        .limit(limit)
        .lean(),
      Campaign.aggregate([
        { $match: matchFilter },
        {
          $group: {
            _id: null,
            total_campaigns: { $sum: 1 },
            total_sent: { $sum: '$stats.sent_count' },
            total_delivered: { $sum: '$stats.delivered_count' },
            total_read: { $sum: '$stats.read_count' }
          }
        }
      ])
    ]);

    const recurringParentIds = campaigns.filter(c => c.is_recurring).map(c => c._id);
    let childStatsMap = {};
    if (recurringParentIds.length > 0) {
      const childAggResults = await Campaign.aggregate([
        { $match: { parent_recurring_id: { $in: recurringParentIds }, deleted_at: null } },
        {
          $group: {
            _id: '$parent_recurring_id',
            sent_count: { $sum: '$stats.sent_count' },
            delivered_count: { $sum: '$stats.delivered_count' },
            read_count: { $sum: '$stats.read_count' },
            failed_count: { $sum: '$stats.failed_count' },
            total_recipients: { $max: '$stats.total_recipients' }
          }
        }
      ]);
      childAggResults.forEach(r => {
        childStatsMap[r._id.toString()] = r;
      });
    }

    const campaignStats = campaignStatsAgg[0] || {
      total_campaigns: 0,
      total_sent: 0,
      total_delivered: 0,
      total_read: 0
    };

    const formattedCampaigns = campaigns.map(c => {
      let stats = c.stats;
      if (c.is_recurring && childStatsMap[c._id.toString()]) {
        const cs = childStatsMap[c._id.toString()];
        stats = {
          ...c.stats,
          sent_count: cs.sent_count,
          delivered_count: cs.delivered_count,
          read_count: cs.read_count,
          failed_count: cs.failed_count,
          total_recipients: cs.total_recipients || c.stats?.total_recipients || 0,
          pending_count: Math.max(0, (cs.total_recipients || c.stats?.total_recipients || 0) - cs.sent_count - cs.failed_count)
        };
      }
      return {
        id: c._id,
        name: c.name,
        description: c.description,
        template_name: c.template_id?.template_name || null,
        recipient_type: c.recipient_type,
        is_published: c.is_published,
        is_scheduled: c.is_scheduled,
        is_recurring: c.is_recurring,
        scheduled_at: c.scheduled_at,
        sent_at: c.sent_at,
        stats,
        status: c.status,
        completion_duration_seconds: c.completion_duration_seconds,
        platform: c.platform
      };
    });

    return res.json({
      success: true,
      data: {
        campaigns: formattedCampaigns,
        campaignStatistics: {
          totalCampaignsCreated: campaignStats.total_campaigns,
          totalSent: campaignStats.total_sent,
          messagesDelivered: campaignStats.total_delivered,
          messagesRead: campaignStats.total_read
        },
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalCount / limit),
          totalItems: totalCount,
          itemsPerPage: limit
        }
      }
    });

  } catch (error) {
    console.error('Error getting campaigns:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to get campaigns',
      details: error.message
    });
  }
};


export const getCampaignById = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);

    const page = Math.max(1, parseInt(req.query.recipient_page || req.query.page) || 1);
    const limit = Math.max(1, Math.min(100, parseInt(req.query.recipient_limit || req.query.limit) || 20));
    const skip = (page - 1) * limit;

    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const excludeRecipients = req.query.exclude_recipients === 'true';

    const campaign = await Campaign.findOne(query)
      .select('-recipients')
      .populate('template_id')
      .populate('waba_id', 'whatsapp_business_account_id')
      .populate('specific_contacts', 'name phone_number')
      .populate('tag_ids', 'label color')
      .populate('segment_ids', 'name')
      .lean();

    if (!campaign) {
      return res.status(404).json({
        success: false,
        error: 'Campaign not found'
      });
    }

    let recipients = [];
    if (!excludeRecipients) {
      const recipientsDoc = await Campaign.findOne(
        { _id: id },
        { recipients: { $slice: [skip, limit] } }
      ).lean();
      recipients = recipientsDoc?.recipients || [];
    }

    let processedRecipients = recipients;
    let processedErrorLog = campaign.error_log || [];

    const uniqueErrors = new Set();
    recipients.forEach(rec => {
      if (rec.failure_reason && typeof rec.failure_reason === 'string') {
        uniqueErrors.add(rec.failure_reason);
      }
    });
    if (campaign.error_log && Array.isArray(campaign.error_log)) {
      campaign.error_log.forEach(log => {
        if (log.error && typeof log.error === 'string') {
          uniqueErrors.add(log.error);
        }
      });
    }

    if (uniqueErrors.size > 0) {
      try {
        const userSettings = await UserSetting.findOne({ user_id: userId }) || await UserSetting.findOne({ user_id: req.user.id });
        if (userSettings && userSettings.ai_model && userSettings.api_key) {
          const aiModel = await AIModel.findOne({
            _id: userSettings.ai_model,
            status: 'active',
            deleted_at: null
          });
          if (aiModel) {
            const errorList = Array.from(uniqueErrors);
            const prompt = `You are a system administrator. Make the following system/WhatsApp error messages user-friendly and actionable for non-technical users.
Return ONLY a valid JSON object where keys are the original error messages and values are the user-friendly versions. Do not include any markdown formatting (like \`\`\`json), comments, or text outside the JSON.
Original Error Messages:
${JSON.stringify(errorList, null, 2)}`;

            const aiResponse = await callAIModel(userId, aiModel, userSettings.api_key, prompt);
            
            try {
              let cleanedResponse = aiResponse.trim();
              if (cleanedResponse.startsWith('```')) {
                cleanedResponse = cleanedResponse.replace(/^```json\s*/, '').replace(/```$/, '').trim();
              }
              const parsed = JSON.parse(cleanedResponse);
              if (parsed && typeof parsed === 'object') {
                processedRecipients = recipients.map(rec => {
                  if (rec.failure_reason && typeof rec.failure_reason === 'string' && parsed[rec.failure_reason]) {
                    return {
                      ...rec,
                      failure_reason: {
                        original: rec.failure_reason,
                        friendly: parsed[rec.failure_reason]
                      }
                    };
                  }
                  return rec;
                });

                processedErrorLog = processedErrorLog.map(log => {
                  if (log.error && typeof log.error === 'string' && parsed[log.error]) {
                    return {
                      ...log,
                      error: {
                        original: log.error,
                        friendly: parsed[log.error]
                      }
                    };
                  }
                  return log;
                });
              }
            } catch (parseError) {
              console.error('Failed to parse AI friendly errors response:', parseError);
            }
          }
        }
      } catch (aiError) {
        console.error('Failed to fetch friendly errors from AI model:', aiError);
      }
    }

    if (campaign.is_recurring) {
      const childRuns = await Campaign.find(
        { parent_recurring_id: new mongoose.Types.ObjectId(id), deleted_at: null },
        { 'stats': 1, 'sent_at': 1, 'created_at': 1 }
      ).sort({ created_at: 1 }).lean();

      if (childRuns.length > 0) {
        const totalSent = childRuns.reduce((s, r) => s + (r.stats?.sent_count || 0), 0);
        const totalDelivered = childRuns.reduce((s, r) => s + (r.stats?.delivered_count || 0), 0);
        const totalRead = childRuns.reduce((s, r) => s + (r.stats?.read_count || 0), 0);
        const totalFailed = childRuns.reduce((s, r) => s + (r.stats?.failed_count || 0), 0);
        const perRunRecipients = childRuns[0]?.stats?.total_recipients || campaign.stats?.total_recipients || 0;

        campaign.stats = {
          ...campaign.stats,
          sent_count: totalSent,
          delivered_count: totalDelivered,
          read_count: totalRead,
          failed_count: totalFailed,
          total_recipients: perRunRecipients,
          total_messages_sent: totalSent, // cumulative across all runs
          runs_completed: childRuns.length,
          pending_count: Math.max(0, perRunRecipients - (childRuns[childRuns.length - 1]?.stats?.sent_count || 0) - (childRuns[childRuns.length - 1]?.stats?.failed_count || 0))
        };
      }

    if (!excludeRecipients) {
        const childRunIds = childRuns.map(r => r._id);
        const totalRunMessages = childRuns.reduce((s, r) => s + (r.stats?.total_recipients || 0), 0);

       const allRunsWithRecipients = [];
        let runIndex = 0;
        for (const run of [...childRuns].reverse()) { // newest run first
          runIndex++;
          const runRecipients = await Campaign.findOne(
            { _id: run._id },
            { recipients: 1, sent_at: 1, created_at: 1 }
          ).lean();
          if (runRecipients?.recipients) {
            for (const r of runRecipients.recipients) {
              allRunsWithRecipients.push({
                ...r,
                run_number: childRuns.length - runIndex + 1,
                run_sent_at: run.sent_at || run.created_at
              });
            }
          }
        }

        processedRecipients = allRunsWithRecipients.slice(skip, skip + limit);

        campaign._recurring_total_messages = totalRunMessages;
      }
    }

    return res.json({
      success: true,
      data: {
        ...campaign,
        error_log: campaign.error_log || [],
        recipients,
        recipients_pagination: excludeRecipients ? null : {
          currentPage: page,
          totalPages: Math.ceil((campaign._recurring_total_messages || campaign.stats?.total_recipients || 0) / limit),
          totalItems: campaign._recurring_total_messages || campaign.stats?.total_recipients || 0,
          itemsPerPage: limit
        }
      }
    });

  } catch (error) {
    console.error('Error getting campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to get campaign',
      details: error.message
    });
  }
};

export const updateCampaign = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const updateData = req.body;

    delete updateData.user_id;
    delete updateData.stats;
    delete updateData.recipients;
    delete updateData.sent_at;

    if (updateData.media_url !== undefined) {
      if (updateData.media_url) {
        try {
          const decodedPath = decodeURIComponent(updateData.media_url);
          const urlWithoutQuery = decodedPath.split('?')[0];
          const extractedFilename = path.basename(urlWithoutQuery);
          if (extractedFilename && extractedFilename.includes('.')) {
            updateData.original_filename = extractedFilename.replace(/-\d{10,13}(?=\.\w+$)/, '');
          }
        } catch (err) {
          // ignore
        }
      } else {
        updateData.original_filename = null;
      }
    }

    if (updateData.segment_ids && typeof updateData.segment_ids === 'string') {
      updateData.segment_ids = JSON.parse(updateData.segment_ids);
    }
    if (updateData.tag_ids && typeof updateData.tag_ids === 'string') {
      updateData.tag_ids = JSON.parse(updateData.tag_ids);
    }
    if (updateData.specific_contacts && typeof updateData.specific_contacts === 'string') {
      updateData.specific_contacts = JSON.parse(updateData.specific_contacts);
    }

    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);
    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const campaign = await Campaign.findOne(query);

    if (!campaign) {
      return res.status(404).json({
        success: false,
        error: 'Campaign not found'
      });
    }

    if (['sending', 'completed', 'failed'].includes(campaign.status)) {
      return res.status(400).json({
        success: false,
        error: 'Cannot update campaign that is already in progress or completed'
      });
    }

    let newIsPublished = campaign.is_published;
    if (updateData.is_published !== undefined) {
      const isPublishedUpdate =
        updateData.is_published === true ||
        updateData.is_published === 'true' ||
        updateData.is_published === 1 ||
        updateData.is_published === '1';

      if (campaign.is_published && !isPublishedUpdate) {
        return res.status(400).json({
          success: false,
          error: 'Cannot change a published campaign back to draft'
        });
      }
      newIsPublished = isPublishedUpdate;
      updateData.is_published = newIsPublished;
    }

    let newIsScheduled = campaign.is_scheduled;
    let newScheduledAt = campaign.scheduled_at;

    if (updateData.is_scheduled !== undefined) {
      newIsScheduled =
        updateData.is_scheduled === true ||
        updateData.is_scheduled === 'true' ||
        updateData.is_scheduled === 1 ||
        updateData.is_scheduled === '1';
      updateData.is_scheduled = newIsScheduled;
    }

    if (updateData.scheduled_at !== undefined) {
      if (updateData.scheduled_at) {
        const parsedDate = new Date(updateData.scheduled_at);
        if (isNaN(parsedDate.getTime())) {
          return res.status(400).json({
            success: false,
            error: 'Invalid scheduled time format'
          });
        }
        newScheduledAt = parsedDate;
      } else {
        newScheduledAt = null;
      }
      updateData.scheduled_at = newScheduledAt;
    }

    if (newIsScheduled) {
      if (!newScheduledAt) {
        return res.status(400).json({
          success: false,
          error: 'Scheduled time is required when enabling scheduling'
        });
      }
      if (newIsPublished && newScheduledAt < new Date()) {
        return res.status(400).json({
          success: false,
          error: 'Scheduled time has passed. Do you want to send immediately or reschedule?'
        });
      }
    } else {
      updateData.scheduled_at = null;
      newScheduledAt = null;
    }

    let newIsRecurring = campaign.is_recurring;
    if (updateData.is_recurring !== undefined) {
      newIsRecurring =
        updateData.is_recurring === true ||
        updateData.is_recurring === 'true' ||
        updateData.is_recurring === 1 ||
        updateData.is_recurring === '1';
      updateData.is_recurring = newIsRecurring;
    }

    if (newIsRecurring) {
      if (!newIsScheduled) {
        return res.status(400).json({ success: false, error: 'Recurring campaigns must have a scheduled start time.' });
      }

      const pattern = updateData.recurring_pattern !== undefined ? updateData.recurring_pattern : campaign.recurring_pattern;
      if (!pattern) {
        return res.status(400).json({ success: false, error: 'recurring_pattern is required for recurring campaigns' });
      }

      const cronExpr = updateData.cron_expression !== undefined ? updateData.cron_expression : campaign.cron_expression;
      if (pattern === 'custom_cron' && !cronExpr) {
        return res.status(400).json({ success: false, error: 'cron_expression is required for custom_cron pattern' });
      }

      if (updateData.recurring_end_date) {
        const parsedEndDate = new Date(updateData.recurring_end_date);
        if (isNaN(parsedEndDate.getTime())) {
          return res.status(400).json({ success: false, error: 'Invalid recurring end date format' });
        }
        updateData.recurring_end_date = parsedEndDate;
      }

      if (newIsPublished && newScheduledAt) {
        updateData.next_run_at = newScheduledAt;
      }
    } else {
      updateData.is_recurring = false;
      updateData.recurring_pattern = null;
      updateData.cron_expression = null;
      updateData.recurring_end_date = null;
      updateData.next_run_at = null;
    }

    updateData.status = newIsPublished ? (newIsRecurring ? 'recurring' : (newIsScheduled ? 'scheduled' : 'draft')) : 'draft';

    if (updateData.avoid_unsubscribers !== undefined) {
      updateData.avoid_unsubscribers = typeof updateData.avoid_unsubscribers === 'string'
        ? updateData.avoid_unsubscribers === 'true'
        : updateData.avoid_unsubscribers;
    }

    const updatedCampaign = await Campaign.findByIdAndUpdate(
      id,
      { ...updateData, updated_at: new Date() },
      { returnDocument: 'after' }
    )
      .select('-recipients')
      .populate('template_id')
      .populate('waba_id', 'whatsapp_business_account_id');

    if (newIsPublished && !newIsScheduled) {
      setImmediate(async () => {
        await processCampaignInBackground(updatedCampaign._id);
      });

      updatedCampaign.status = 'sending';
      updatedCampaign.sent_at = new Date();
    }

    return res.json({
      success: true,
      message: 'Campaign updated successfully',
      data: updatedCampaign
    });

  } catch (error) {
    console.error('Error updating campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to update campaign',
      details: error.message
    });
  }
};

export const deleteCampaign = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);

    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const campaign = await Campaign.findOne(query);

    if (!campaign) {
      return res.status(404).json({
        success: false,
        error: 'Campaign not found'
      });
    }

    if (['sending', 'completed', 'failed'].includes(campaign.status)) {
      return res.status(400).json({
        success: false,
        error: 'Cannot delete campaign that is already in progress or completed'
      });
    }

    await campaign.softDelete();

    return res.json({
      success: true,
      message: 'Campaign deleted successfully'
    });

  } catch (error) {
    console.error('Error deleting campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to delete campaign',
      details: error.message
    });
  }
};

export const publishCampaign = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);
    const { action, scheduled_at } = req.body;

    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const campaign = await Campaign.findOne(query);

    if (!campaign) {
      return res.status(404).json({
        success: false,
        error: 'Campaign not found'
      });
    }

    if (campaign.is_published) {
      return res.status(400).json({
        success: false,
        error: 'Campaign is already published'
      });
    }

    if (['sending', 'completed', 'failed'].includes(campaign.status)) {
      return res.status(400).json({
        success: false,
        error: 'Campaign is already in progress or completed'
      });
    }

    let updateData = {
      is_published: true,
      updated_at: new Date()
    };

    let startSending = false;

    if (campaign.is_scheduled) {
      const scheduledTime = campaign.scheduled_at;
      if (!scheduledTime) {
        return res.status(400).json({
          success: false,
          error: 'Campaign is marked as scheduled but has no scheduled time'
        });
      }

      if (scheduledTime < new Date()) {
        if (action === 'send_immediately') {
          updateData.is_scheduled = false;
          updateData.scheduled_at = null;
          updateData.status = 'draft';
          startSending = true;
        } else if (action === 'reschedule') {
          if (!scheduled_at) {
            return res.status(400).json({
              success: false,
              error: 'New scheduled time is required to reschedule'
            });
          }
          const newScheduledTime = new Date(scheduled_at);
          if (isNaN(newScheduledTime.getTime())) {
            return res.status(400).json({
              success: false,
              error: 'Invalid scheduled time format'
            });
          }
          if (newScheduledTime < new Date()) {
            return res.status(400).json({
              success: false,
              error: 'New scheduled time must be in the future'
            });
          }
          updateData.is_scheduled = true;
          updateData.scheduled_at = newScheduledTime;
          updateData.status = campaign.is_recurring ? 'recurring' : 'scheduled';
        } else {
          return res.status(400).json({
            success: false,
            code: 'SCHEDULED_TIME_PASSED',
            error: 'Scheduled time has passed. Do you want to send immediately or reschedule?',
            scheduled_at: campaign.scheduled_at
          });
        }
      } else {
        updateData.status = campaign.is_recurring ? 'recurring' : 'scheduled';
      }

      if (campaign.is_recurring && updateData.scheduled_at !== null) {
        updateData.next_run_at = updateData.scheduled_at || campaign.scheduled_at;
      }
    } else {
      updateData.status = 'draft';
      startSending = true;
    }

    const updatedCampaign = await Campaign.findByIdAndUpdate(
      id,
      { $set: updateData },
      { returnDocument: 'after' }
    )
      .populate('template_id')
      .populate('waba_id', 'whatsapp_business_account_id');

    if (startSending) {
      setImmediate(async () => {
        await processCampaignInBackground(updatedCampaign._id);
      });
      updatedCampaign.status = 'sending';
      updatedCampaign.sent_at = new Date();
    }

    return res.json({
      success: true,
      message: startSending ? 'Campaign published and sending started' : 'Campaign published successfully',
      data: updatedCampaign
    });

  } catch (error) {
    console.error('Error publishing campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to publish campaign',
      details: error.message
    });
  }
};

export const resendCampaign = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);
    const { is_scheduled, scheduled_at } = req.body;

    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const originalCampaign = await Campaign.findOne(query);

    if (!originalCampaign) {
      return res.status(404).json({
        success: false,
        error: 'Campaign not found'
      });
    }

    if (originalCampaign.status === 'sending') {
      return res.status(400).json({
        success: false,
        error: 'Campaign is currently sending and cannot be resent.'
      });
    }

    const isScheduledBool =
      is_scheduled === true ||
      is_scheduled === 'true' ||
      is_scheduled === 1 ||
      is_scheduled === '1';

    let parsedScheduledAt = null;

    if (isScheduledBool) {
      if (!scheduled_at) {
        return res.status(400).json({
          success: false,
          error: 'Scheduled time is required when scheduling a campaign.'
        });
      }
      parsedScheduledAt = new Date(scheduled_at);
      if (isNaN(parsedScheduledAt.getTime())) {
        return res.status(400).json({
          success: false,
          error: 'Invalid scheduled time format'
        });
      }
      if (parsedScheduledAt < new Date()) {
        return res.status(400).json({
          success: false,
          error: 'Scheduled time must be in the future'
        });
      }
    }

    const rootCampaignId = originalCampaign.original_campaign_id || originalCampaign._id;
    const resendCount = await Campaign.countDocuments({
      original_campaign_id: rootCampaignId
    });

    const rootCampaign = originalCampaign.original_campaign_id
      ? await Campaign.findById(rootCampaignId)
      : originalCampaign;

    const rootName = rootCampaign ? rootCampaign.name : originalCampaign.name;
    const newName = `${rootName} - Resend #${resendCount + 1}`;

    const resetRecipients = originalCampaign.recipients.map(r => ({
      contact_id: r.contact_id,
      phone_number: r.phone_number,
      status: 'pending',
      sent_at: null,
      delivered_at: null,
      read_at: null,
      failed_at: null,
      failure_reason: null,
      message_id: null
    }));

    const newCampaignData = {
      name: newName,
      description: originalCampaign.description,
      workspace_id: originalCampaign.workspace_id,
      user_id: originalCampaign.user_id,
      created_by: req.user.id,
      waba_id: originalCampaign.waba_id,
      platform: originalCampaign.platform,
      template_id: originalCampaign.template_id,
      template_name: originalCampaign.template_name,
      language_code: originalCampaign.language_code,
      recipient_type: originalCampaign.recipient_type,
      specific_contacts: originalCampaign.specific_contacts,
      tag_ids: originalCampaign.tag_ids,
      segment_ids: originalCampaign.segment_ids,
      variables_mapping: originalCampaign.variables_mapping,
      media_url: originalCampaign.media_url,
      coupon_code: originalCampaign.coupon_code,
      location_data: originalCampaign.location_data,
      carousel_products: originalCampaign.carousel_products,
      carousel_cards_data: originalCampaign.carousel_cards_data,
      offer_expiration_minutes: originalCampaign.offer_expiration_minutes,
      avoid_unsubscribers: originalCampaign.avoid_unsubscribers,

      is_published: true,
      is_scheduled: isScheduledBool,
      scheduled_at: parsedScheduledAt,
      status: isScheduledBool ? 'scheduled' : 'draft',

      is_resend: true,
      original_campaign_id: rootCampaignId,

      stats: {
        total_recipients: resetRecipients.length,
        pending_count: resetRecipients.length
      },
      recipients: resetRecipients
    };

    const newCampaign = await Campaign.create(newCampaignData);

    let startSending = !isScheduledBool;

    if (startSending) {
      setImmediate(async () => {
        await processCampaignInBackground(newCampaign._id);
      });
      newCampaign.status = 'sending';
      newCampaign.sent_at = new Date();
    }

    return res.status(201).json({
      success: true,
      message: startSending ? 'Campaign cloned and sending started' : 'Campaign cloned and scheduled successfully',
      data: newCampaign
    });

  } catch (error) {
    console.error('Error resending campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to resend campaign',
      details: error.message
    });
  }
};

export const sendCampaign = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);

    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const campaign = await Campaign.findOne(query)
      .select('status stats')
      .populate('template_id');

    if (!campaign) {
      return res.status(404).json({
        success: false,
        error: 'Campaign not found'
      });
    }

    if (campaign.status !== 'draft') {
      return res.status(400).json({
        success: false,
        error: 'Campaign must be in draft status to send'
      });
    }

    setTimeout(async () => {
      await processCampaignInBackground(campaign._id);
    }, 0);

    return res.json({
      success: true,
      message: 'Campaign sending started',
      data: {
        campaign_id: campaign._id,
        total_recipients: campaign.stats.total_recipients,
        status: 'sending'
      }
    });

  } catch (error) {
    console.error('Error sending campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to send campaign',
      details: error.message
    });
  }
};


export const togglePauseCampaign = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);

    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const campaign = await Campaign.findOne(query);
    if (!campaign) {
      return res.status(404).json({
        success: false,
        error: 'Campaign not found'
      });
    }

    if (!campaign.is_published) {
      return res.status(400).json({
        success: false,
        error: 'Campaign must be published to pause or resume.'
      });
    }

    if (campaign.status !== 'sending') {
      return res.status(400).json({
        success: false,
        error: 'Only running (sending) campaigns can be paused or resumed.'
      });
    }

    if (campaign.is_paused) {
      setImmediate(async () => {
        await processCampaignInBackground(campaign._id);
      });
      campaign.is_paused = false;

      return res.status(200).json({
        success: true,
        message: 'Campaign resumed and sending restarted',
        data: campaign
      });
    } else {
      campaign.is_paused = true;
      await campaign.save();

      try {
        const { getCampaignQueue } = await import('../queues/campaign-queue.js');
        const campaignQueue = getCampaignQueue();
        if (campaignQueue && typeof campaignQueue.getJobs === 'function') {
          const jobs = await campaignQueue.getJobs(['waiting', 'delayed']);
          let removedCount = 0;
          for (const job of jobs) {
            if (job.data?.campaignId === id) {
              await job.remove();
              removedCount++;
            }
          }
          console.log(`[Campaign pause] Removed ${removedCount} waiting/delayed jobs from the queue for campaign ${id}`);
        }
      } catch (queueError) {
        console.error('Failed to clean up queue on pause:', queueError);
      }

      return res.status(200).json({
        success: true,
        message: 'Campaign paused successfully',
        data: campaign
      });
    }
  } catch (error) {
    console.error('Error toggling campaign pause status:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to toggle campaign pause status',
      details: error.message
    });
  }
};

export const setupRecurringCampaign = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const {
      scheduled_at,
      recurring_pattern,
      cron_expression,
      recurring_end_date
    } = req.body;

    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);
    const query = {
      _id: id,
      user_id: userId,
      deleted_at: null
    };
    if (workspaceId) query.workspace_id = workspaceId;

    const campaign = await Campaign.findOne(query);

    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    let parsedScheduledAt = null;

    if (scheduled_at) {
      parsedScheduledAt = new Date(scheduled_at);
      if (isNaN(parsedScheduledAt.getTime())) {
        return res.status(400).json({ success: false, error: 'Invalid scheduled time format' });
      }

      if (parsedScheduledAt < new Date()) {
        return res.status(400).json({ success: false, error: 'Scheduled time must be in the future.' });
      }
    }

    if (!recurring_pattern) {
      return res.status(400).json({ success: false, error: 'recurring_pattern is required.' });
    }

    if (recurring_pattern === 'custom_cron' && !cron_expression) {
      return res.status(400).json({ success: false, error: 'cron_expression is required for custom_cron pattern' });
    }

    if (!parsedScheduledAt) {
      const now = new Date();
      if (recurring_pattern === 'daily') {
        parsedScheduledAt = new Date(now.getTime() + 24 * 60 * 60 * 1000);
      } else if (recurring_pattern === 'weekly') {
        parsedScheduledAt = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
      } else if (recurring_pattern === 'monthly') {
        const nextMonth = new Date(now);
        nextMonth.setMonth(nextMonth.getMonth() + 1);
        parsedScheduledAt = nextMonth;
      } else if (recurring_pattern === 'custom_cron' && cron_expression) {
        try {
          const cronParser = await import('cron-parser');
          const interval = cronParser.default.CronExpressionParser.parse(cron_expression);
          parsedScheduledAt = interval.next().toDate();
        } catch (err) {
          return res.status(400).json({ success: false, error: 'Invalid cron expression' });
        }
      }
    }

    if (recurring_pattern === 'custom_cron' && !cron_expression) {
      return res.status(400).json({ success: false, error: 'cron_expression is required for custom_cron pattern' });
    }

    let parsedRecurringEndDate = null;
    if (recurring_end_date) {
      parsedRecurringEndDate = new Date(recurring_end_date);
      if (isNaN(parsedRecurringEndDate.getTime())) {
        return res.status(400).json({ success: false, error: 'Invalid recurring end date format' });
      }
    }

    campaign.is_scheduled = true;
    campaign.is_recurring = true;
    campaign.scheduled_at = parsedScheduledAt;
    campaign.next_run_at = parsedScheduledAt;
    campaign.recurring_pattern = recurring_pattern;
    campaign.cron_expression = recurring_pattern === 'custom_cron' ? cron_expression : null;
    campaign.recurring_end_date = parsedRecurringEndDate;
    campaign.status = 'recurring';
    campaign.is_published = true;

    await campaign.save();

    return res.json({
      success: true,
      message: 'Campaign successfully converted to recurring setup',
      data: campaign
    });

  } catch (error) {
    console.error('Error setting up recurring campaign:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to setup recurring campaign',
      details: error.message
    });
  }
};

export const getCampaignInsights = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.owner_id;
    const workspaceId = sanitizeWorkspaceId(req.query.workspace_id || req.headers['x-workspace-id']);

    const matchFilter = {
      _id: new mongoose.Types.ObjectId(id),
      user_id: new mongoose.Types.ObjectId(userId),
      deleted_at: null
    };
    if (workspaceId) matchFilter.workspace_id = new mongoose.Types.ObjectId(workspaceId);

    const campaign = await Campaign.findOne(matchFilter)
      .select('name status stats sent_at created_at template_id template_name language_code variables_mapping media_url carousel_cards_data')
      .populate('template_id');

    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    const fullCampaign = await Campaign.findById(id).select('recipients name status stats sent_at created_at updated_at completed_at template_id template_name language_code variables_mapping media_url carousel_cards_data platform total_cost events button_interactions is_scheduled scheduled_at is_recurring recurring_pattern cron_expression next_run_at recurring_end_date').populate('template_id').lean();

    const recipients = fullCampaign?.recipients || [];
    const recipientContactIds = recipients.map(r => r.contact_id).filter(Boolean);

    const sentAtDate = campaign.sent_at || campaign.created_at || new Date();
    const repliesCount = await Contact.countDocuments({
      _id: { $in: recipientContactIds },
      last_incoming_message_at: { $gte: sentAtDate }
    });

    let aggregatedStats = null;
    // For recurring parent campaigns, aggregate stats from all child runs for accurate funnel numbers
    if (fullCampaign?.is_recurring) {
      const childAgg = await Campaign.aggregate([
        { $match: { parent_recurring_id: new mongoose.Types.ObjectId(id), deleted_at: null } },
        {
          $group: {
            _id: null,
            sent_count: { $sum: '$stats.sent_count' },
            delivered_count: { $sum: '$stats.delivered_count' },
            read_count: { $sum: '$stats.read_count' },
            failed_count: { $sum: '$stats.failed_count' },
            total_recipients: { $max: '$stats.total_recipients' }
          }
        }
      ]);
      if (childAgg.length > 0) aggregatedStats = childAgg[0];
    }

    const targeted = aggregatedStats?.total_recipients ?? campaign.stats?.total_recipients ?? recipients.length ?? 0;
    const sent = aggregatedStats?.sent_count ?? campaign.stats?.sent_count ?? 0;
    const delivered = aggregatedStats?.delivered_count ?? campaign.stats?.delivered_count ?? 0;
    const read = aggregatedStats?.read_count ?? campaign.stats?.read_count ?? 0;
    const failed = aggregatedStats?.failed_count ?? campaign.stats?.failed_count ?? 0;
    const pending = campaign.stats?.pending_count ?? 0;
    const queued = campaign.stats?.queued_count ?? 0;

    const deliveryRateVal = targeted > 0 ? parseFloat(((delivered / targeted) * 100).toFixed(1)) : 0;
    const readRateVal = delivered > 0 ? parseFloat(((read / delivered) * 100).toFixed(1)) : 0;
    const replyRateVal = targeted > 0 ? parseFloat(((repliesCount / targeted) * 100).toFixed(1)) : 0;
    const failureRateVal = targeted > 0 ? parseFloat(((failed / targeted) * 100).toFixed(1)) : 0;

    const dRate = targeted > 0 ? delivered / targeted : 0;
    const rRate = delivered > 0 ? read / delivered : 0;
    const fRate = targeted > 0 ? failed / targeted : 0;
    const repRate = targeted > 0 ? repliesCount / targeted : 0;
    const healthScore = Math.round(dRate * 40 + rRate * 30 + (1 - fRate) * 20 + repRate * 10);

    let totalCost = fullCampaign?.total_cost || fullCampaign?.stats?.total_cost || 0;
    let metaRateCards = [
      { country: 'US', name: 'United States', code: '1', MARKETING: 0.0250, UTILITY: 0.0150, AUTHENTICATION: 0.0125 },
      { country: 'CA', name: 'Canada', code: '1', MARKETING: 0.0250, UTILITY: 0.0150, AUTHENTICATION: 0.0125 },
      { country: 'IN', name: 'India', code: '91', MARKETING: 0.0099, UTILITY: 0.0042, AUTHENTICATION: 0.0028 },
      { country: 'GB', name: 'United Kingdom', code: '44', MARKETING: 0.0385, UTILITY: 0.0210, AUTHENTICATION: 0.0190 },
      { country: 'BR', name: 'Brazil', code: '55', MARKETING: 0.0625, UTILITY: 0.0350, AUTHENTICATION: 0.0315 },
      { country: 'MX', name: 'Mexico', code: '52', MARKETING: 0.0430, UTILITY: 0.0240, AUTHENTICATION: 0.0215 },
      { country: 'DE', name: 'Germany', code: '49', MARKETING: 0.1135, UTILITY: 0.0607, AUTHENTICATION: 0.0536 },
      { country: 'FR', name: 'France', code: '33', MARKETING: 0.0880, UTILITY: 0.0470, AUTHENTICATION: 0.0415 },
      { country: 'IT', name: 'Italy', code: '39', MARKETING: 0.0691, UTILITY: 0.0370, AUTHENTICATION: 0.0326 },
      { country: 'ES', name: 'Spain', code: '34', MARKETING: 0.0664, UTILITY: 0.0355, AUTHENTICATION: 0.0313 },
      { country: 'AU', name: 'Australia', code: '61', MARKETING: 0.0503, UTILITY: 0.0269, AUTHENTICATION: 0.0237 },
      { country: 'JP', name: 'Japan', code: '81', MARKETING: 0.0717, UTILITY: 0.0384, AUTHENTICATION: 0.0338 },
      { country: 'AE', name: 'United Arab Emirates', code: '971', MARKETING: 0.0341, UTILITY: 0.0182, AUTHENTICATION: 0.0161 },
      { country: 'ZA', name: 'South Africa', code: '27', MARKETING: 0.0382, UTILITY: 0.0204, AUTHENTICATION: 0.0180 },
      { country: 'SG', name: 'Singapore', code: '65', MARKETING: 0.0520, UTILITY: 0.0278, AUTHENTICATION: 0.0245 },
      { country: 'SA', name: 'Saudi Arabia', code: '966', MARKETING: 0.0412, UTILITY: 0.0220, AUTHENTICATION: 0.0194 },
      { country: 'ID', name: 'Indonesia', code: '62', MARKETING: 0.0325, UTILITY: 0.0173, AUTHENTICATION: 0.0153 },
      { country: 'MY', name: 'Malaysia', code: '60', MARKETING: 0.0390, UTILITY: 0.0208, AUTHENTICATION: 0.0184 },
      { country: 'DEFAULT', name: 'Global Default / Rest of World', code: 'DEFAULT', MARKETING: 0.0520, UTILITY: 0.0280, AUTHENTICATION: 0.0250 }
    ];

    try {
      if (fullCampaign?.waba_id) {
        const waba = await WhatsappWaba.findById(fullCampaign.waba_id);
        const wabaAccountId = waba?.whatsapp_business_account_id;
        const token = waba?.access_token || waba?.system_user_access_token || process.env.META_ACCESS_TOKEN;
        if (wabaAccountId && token) {
          const rateCardRes = await axios.get(`https://graph.facebook.com/${process.env.API_VERSION || 'v19.0'}/${wabaAccountId}/rate_card?currency=USD`, {
            headers: { Authorization: `Bearer ${token}` }
          });
          if (rateCardRes.data?.data?.[0]?.rates) {
            const liveRates = rateCardRes.data.data[0].rates;
            const rateMap = {};
            liveRates.forEach(r => {
              const c = r.country || 'DEFAULT';
              if (!rateMap[c]) {
                rateMap[c] = {
                  country: c,
                  name: c === 'US' ? 'United States' : c === 'IN' ? 'India' : c === 'GB' ? 'United Kingdom' : c === 'BR' ? 'Brazil' : c === 'MX' ? 'Mexico' : c === 'DE' ? 'Germany' : c === 'FR' ? 'France' : c === 'IT' ? 'Italy' : c === 'ES' ? 'Spain' : c === 'AU' ? 'Australia' : c === 'JP' ? 'Japan' : c === 'AE' ? 'United Arab Emirates' : c === 'ZA' ? 'South Africa' : c === 'SG' ? 'Singapore' : c,
                  code: r.country_calling_code || 'DEFAULT',
                  MARKETING: 0.025,
                  UTILITY: 0.015,
                  AUTHENTICATION: 0.0125
                };
              }
              const cat = r.category?.toUpperCase();
              if (cat && r.rate != null) {
                rateMap[c][cat] = parseFloat(r.rate);
              }
            });
            metaRateCards = Object.values(rateMap);
          }
        }
      }
    } catch (err) {
      console.error('Meta Rate Card API fetch failed, using official fallback matrix:', err.message);
    }

    if (!totalCost && recipients.length > 0) {
      const category = (fullCampaign?.template_id?.category || 'MARKETING').toUpperCase();
      const rateMapByCode = {};
      metaRateCards.forEach(mc => { rateMapByCode[mc.code] = mc; });

      totalCost = recipients.reduce((sum, r) => {
        if (r.status !== 'delivered' && r.status !== 'read') return sum;
        if (r.cost) return sum + r.cost;
        const cleanedPhone = (r.phone_number || '').toString().replace(/\D/g, '');
        let countryPrefix = 'DEFAULT';
        for (const prefix of Object.keys(rateMapByCode)) {
          if (prefix !== 'DEFAULT' && cleanedPhone.startsWith(prefix)) {
            countryPrefix = prefix;
            break;
          }
        }
        const rates = rateMapByCode[countryPrefix] || rateMapByCode['DEFAULT'] || metaRateCards[0];
        const calculatedCost = rates[category] || rates['MARKETING'] || 0.052;
        return sum + calculatedCost;
      }, 0);
      totalCost = parseFloat(totalCost.toFixed(4));
    }
    const costPerRead = read > 0 ? parseFloat((totalCost / read).toFixed(4)) : 0.00;

    const timeline = [];
    if (fullCampaign?.events && fullCampaign.events.length > 0) {
      timeline.push(...fullCampaign.events);
    } else {
      if (fullCampaign?.created_at) {
        timeline.push({ type: 'created', title: 'Campaign Created', created_at: fullCampaign.created_at });
      }
      if (fullCampaign?.sent_at) {
        timeline.push({ type: 'sent', title: 'Broadcast Dispatched', created_at: fullCampaign.sent_at });
      }
      let firstDelivered = null;
      let firstRead = null;
      recipients.forEach(r => {
        if (r.delivered_at && (!firstDelivered || r.delivered_at < firstDelivered)) firstDelivered = r.delivered_at;
        if (r.read_at && (!firstRead || r.read_at < firstRead)) firstRead = r.read_at;
      });
      if (firstDelivered) {
        timeline.push({ type: 'delivered', title: 'First Message Delivered', created_at: firstDelivered });
      }
      if (firstRead) {
        timeline.push({ type: 'read', title: 'First Message Read', created_at: firstRead });
      }
    }

    const hourlyAnalytics = Array.from({ length: 24 }, (_, i) => ({
      hour: i.toString().padStart(2, '0'),
      delivered: 0,
      read: 0,
      replies: 0
    }));

    recipients.forEach(r => {
      if (r.delivered_at) {
        const h = new Date(r.delivered_at).getHours();
        if (!isNaN(h)) hourlyAnalytics[h].delivered++;
      }
      if (r.read_at) {
        const h = new Date(r.read_at).getHours();
        if (!isNaN(h)) hourlyAnalytics[h].read++;
      }
    });

    const statusDistribution = {
      sent,
      queued,
      pending,
      delivered,
      read,
      failed
    };

    const failureAggregation = await Campaign.aggregate([
      { $match: matchFilter },
      { $unwind: '$recipients' },
      { $match: { 'recipients.status': 'failed' } },
      {
        $group: {
          _id: { $ifNull: ['$recipients.failure_reason', 'Unknown Error'] },
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1 } }
    ]);

    const totalFailedForPct = failureAggregation.reduce((acc, f) => acc + f.count, 0) || 1;
    const failureDiagnostics = failureAggregation.map(f => ({
      reason: f._id,
      count: f.count,
      percentage: parseFloat(((f.count / totalFailedForPct) * 100).toFixed(1))
    }));

    const interactiveBreakdown = fullCampaign?.button_interactions || fullCampaign?.stats?.button_interactions || [];

    const topRecipients = recipients.slice(0, 50).map(r => ({
      contact: r.name || "Contact",
      phone: r.phone_number,
      status: r.status,
      reason: r.failure_reason || ""
    }));

    const startedAt = fullCampaign?.sent_at || fullCampaign?.created_at;
    const completedAt = fullCampaign?.completed_at || (fullCampaign?.status === 'completed' ? fullCampaign?.updated_at : null);
    const durationSeconds = completedAt && startedAt ? Math.round((new Date(completedAt).getTime() - new Date(startedAt).getTime()) / 1000) : null;

    const platform = fullCampaign?.platform || 'whatsapp';
    const engagementData = platform === 'whatsapp' ? {
      delivered,
      read,
      replies: repliesCount,
      conversation_cost: totalCost
    } : {
      replies: repliesCount,
      reactions: fullCampaign?.stats?.reactions_count || 0,
      story_replies: fullCampaign?.stats?.story_replies_count || 0,
      comments: fullCampaign?.stats?.comments_count || 0
    };

    const benchmarking = {
      account_average_read_rate: 65.0,
      campaign_read_rate: readRateVal,
      performance: readRateVal >= 65.0 ? "Above Average" : "Below Average"
    };

    return res.json({
      success: true,
      data: {
        campaign_info: {
          id: campaign._id,
          name: campaign.name,
          status: campaign.status,
          sent_at: campaign.sent_at,
          started_at: startedAt,
          completed_at: completedAt,
          duration_seconds: durationSeconds,
          template_name: campaign.template_name,
          language_code: campaign.language_code,
          platform: platform,
          is_scheduled: fullCampaign?.is_scheduled,
          scheduled_at: fullCampaign?.scheduled_at,
          is_recurring: fullCampaign?.is_recurring,
          recurring_pattern: fullCampaign?.recurring_pattern,
          cron_expression: fullCampaign?.cron_expression,
          next_run_at: fullCampaign?.next_run_at,
          recurring_end_date: fullCampaign?.recurring_end_date,
          updated_at: fullCampaign?.updated_at
        },
        summary_cards: {
          targeted,
          sent,
          delivered,
          read,
          failed,
          replies: repliesCount
        },
        rates: {
          delivery_rate: deliveryRateVal,
          read_rate: readRateVal,
          reply_rate: replyRateVal,
          failure_rate: failureRateVal
        },
        health_score: healthScore,
        engagement: engagementData,
        timeline,
        hourly_analytics: hourlyAnalytics,
        status_distribution: statusDistribution,
        failure_diagnostics: failureDiagnostics,
        interactive_breakdown: interactiveBreakdown,
        top_recipients: topRecipients,
        cost_analytics: {
          total_cost: totalCost,
          cost_per_read: costPerRead,
          meta_rate_cards: metaRateCards,
          template_category: fullCampaign?.template_id?.category || 'MARKETING'
        },
        benchmarking,
        template_preview: {
          template: campaign.template_id,
          variables_mapping: campaign.variables_mapping,
          media_url: campaign.media_url,
          carousel_cards_data: campaign.carousel_cards_data
        }
      }
    });

  } catch (error) {
    console.error('Error fetching campaign insights:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to fetch campaign insights',
      details: error.message
    });
  }
};

export default {
  createCampaign,
  getAllCampaigns,
  getCampaignById,
  updateCampaign,
  deleteCampaign,
  sendCampaign,
  publishCampaign,
  resendCampaign,
  togglePauseCampaign,
  setupRecurringCampaign,
  getCampaignInsights
};
