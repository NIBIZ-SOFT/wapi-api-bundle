import Campaign from '../models/campaign.model.js';
import Message from '../models/message.model.js';
import mongoose from 'mongoose';



export const updateCampaignStatsFromMessage = async (messageId, newStatus, timestamp = null, failureReason = null) => {
  try {
    const message = await Message.findById(messageId)
      .select('template_id metadata recipient_number contact_id user_id')
      .lean();

    if (!message) {
      console.log(`Message not found: ${messageId}`);
      return null;
    }

    const campaignId = message.metadata?.campaign_id;
    if (!campaignId) {
      console.log(`Message ${messageId} is not part of a campaign`);
      return null;
    }


    const recipientQuery = message.contact_id
      ? { _id: campaignId, "recipients.contact_id": message.contact_id }
      : { _id: campaignId, "recipients.phone_number": message.recipient_number };

    const currentCampaign = await Campaign.findOne(
      recipientQuery,
      {
        stats: 1,
        status: 1,
        sent_at: 1,
        recipients: {
          $elemMatch: message.contact_id
            ? { contact_id: message.contact_id }
            : { phone_number: message.recipient_number }
        }
      }
    ).lean();

    if (!currentCampaign) {
      console.log(`Campaign ${campaignId} or recipient not found`);
      return null;
    }

    const recipient = currentCampaign.recipients && currentCampaign.recipients[0];
    if (!recipient) {
      console.log(`Recipient not found in campaign ${campaignId}`);
      return null;
    }

    const oldStatus = recipient.status || 'pending';
    
    // Define the valid hierarchy to prevent out-of-order webhooks from overwriting more advanced statuses
    const statusSeverity = {
      'pending': 0,
      'sent': 1,
      'delivered': 2,
      'read': 3,
      'failed': 4
    };

    const isStatusDowngrade = statusSeverity[oldStatus] >= statusSeverity[newStatus];
    
    // Exception: we can transition to 'failed' from any status other than 'failed' itself
    const canTransition = !isStatusDowngrade || (newStatus === 'failed' && oldStatus !== 'failed');

    if (!canTransition) {
      console.log(`Skipping status update for campaign ${campaignId} - status already processed or downgrade prevented: ${oldStatus} -> ${newStatus}`);
      return {
        campaignId: currentCampaign._id,
        stats: currentCampaign.stats,
        status: currentCampaign.status
      };
    }

    const recipientUpdate = {
      $set: {
        "recipients.$.status": newStatus,
        "recipients.$.updated_at": new Date()
      }
    };

    const now = timestamp || new Date();
    if (newStatus === 'sent') recipientUpdate.$set["recipients.$.sent_at"] = now;
    if (newStatus === 'delivered') recipientUpdate.$set["recipients.$.delivered_at"] = now;
    if (newStatus === 'read') recipientUpdate.$set["recipients.$.read_at"] = now;
    if (newStatus === 'failed') {
      recipientUpdate.$set["recipients.$.failed_at"] = now;
      if (failureReason) {
        recipientUpdate.$set["recipients.$.failure_reason"] = failureReason;
      }
    }

    const incUpdate = {};

    if (newStatus === 'sent' && oldStatus === 'pending') {
      incUpdate['stats.sent_count'] = 1;
      incUpdate['stats.pending_count'] = -1;
    } 
    else if (newStatus === 'delivered') {
      if (oldStatus === 'pending') {
        incUpdate['stats.delivered_count'] = 1;
        incUpdate['stats.sent_count'] = 1;
        incUpdate['stats.pending_count'] = -1;
      } else if (oldStatus === 'sent') {
        incUpdate['stats.delivered_count'] = 1;
      }
    } 
    else if (newStatus === 'read') {
      if (oldStatus === 'pending') {
        incUpdate['stats.read_count'] = 1;
        incUpdate['stats.delivered_count'] = 1;
        incUpdate['stats.sent_count'] = 1;
        incUpdate['stats.pending_count'] = -1;
      } else if (oldStatus === 'sent') {
        incUpdate['stats.read_count'] = 1;
        incUpdate['stats.delivered_count'] = 1;
      } else if (oldStatus === 'delivered') {
        incUpdate['stats.read_count'] = 1;
      }
    } 
    else if (newStatus === 'failed') {
      if (oldStatus === 'pending') {
        incUpdate['stats.failed_count'] = 1;
        incUpdate['stats.pending_count'] = -1;
      } else if (oldStatus === 'sent') {
        incUpdate['stats.failed_count'] = 1;
        incUpdate['stats.sent_count'] = -1;
      } else if (oldStatus === 'delivered') {
        incUpdate['stats.failed_count'] = 1;
        incUpdate['stats.delivered_count'] = -1;
        incUpdate['stats.sent_count'] = -1;
      } else if (oldStatus === 'read') {
        incUpdate['stats.failed_count'] = 1;
        incUpdate['stats.read_count'] = -1;
        incUpdate['stats.delivered_count'] = -1;
        incUpdate['stats.sent_count'] = -1;
      }
    }

    let updatedCampaign;
    if (Object.keys(incUpdate).length > 0) {
      console.log(`Incrementing campaign ${campaignId} stats atomically:`, incUpdate);
      updatedCampaign = await Campaign.findOneAndUpdate(
        recipientQuery,
        { ...recipientUpdate, $inc: incUpdate },
        { new: true, select: 'stats status sent_at', runValidators: true }
      ).lean();
    } else {
      console.log(`Updating recipient state without changing counters for campaign ${campaignId}`);
      updatedCampaign = await Campaign.findOneAndUpdate(
        recipientQuery,
        recipientUpdate,
        { new: true, select: 'stats status sent_at' }
      ).lean();
    }

    if (!updatedCampaign) {
      console.log(`Campaign not found for ID: ${campaignId}`);
      return null;
    }

    const totalRecipients = updatedCampaign.stats.total_recipients || 0;
    const sentCount = updatedCampaign.stats.sent_count || 0;
    const deliveredCount = updatedCampaign.stats.delivered_count || 0;
    const readCount = updatedCampaign.stats.read_count || 0;
    const failedCount = updatedCampaign.stats.failed_count || 0;

    let newCampaignStatus = updatedCampaign.status;
    if (failedCount > 0) {
      newCampaignStatus = 'completed_with_errors';
    } else if (readCount === totalRecipients && totalRecipients > 0) {
      newCampaignStatus = 'completed';
    } else if (deliveredCount + readCount === totalRecipients && totalRecipients > 0) {
      newCampaignStatus = 'delivered';
    }

    const finalUpdate = {};
    if (newCampaignStatus !== updatedCampaign.status) {
      finalUpdate.status = newCampaignStatus;
    }

    const totalProcessed = sentCount + failedCount;
    if (totalProcessed >= totalRecipients && updatedCampaign.status === 'sending') {
      const hasFailures = failedCount > 0;
      const finalStatus = hasFailures ? 'completed_with_errors' : 'completed';
      finalUpdate.status = finalStatus;
      finalUpdate.completed_at = new Date();

      if (updatedCampaign.sent_at) {
        const duration = Math.floor((Date.now() - new Date(updatedCampaign.sent_at).getTime()) / 1000);
        finalUpdate.completion_duration_seconds = duration;
      }
    }

    if (Object.keys(finalUpdate).length > 0) {
      finalUpdate.updated_at = new Date();
      await Campaign.findByIdAndUpdate(campaignId, { $set: finalUpdate });
      updatedCampaign.status = finalUpdate.status || updatedCampaign.status;
    }

    console.log(`Campaign ${campaignId} stats updated for message ${messageId}: ${newStatus}`);

    return {
      campaignId: updatedCampaign._id,
      stats: updatedCampaign.stats,
      status: updatedCampaign.status
    };

  } catch (error) {
    console.error('Error updating campaign stats from message:', error);
    throw error;
  }
};


export const bulkUpdateCampaignStats = async (messageIds, status, timestamp = null) => {
  try {
    const results = [];

    for (const messageId of messageIds) {
      try {
        const result = await updateCampaignStatsFromMessage(messageId, status, timestamp);
        if (result) {
          results.push(result);
        }
      } catch (error) {
        console.error(`Error updating campaign stats for message ${messageId}:`, error);
      }
    }

    return {
      processed: results.length,
      total: messageIds.length,
      results
    };

  } catch (error) {
    console.error('Error in bulk campaign stats update:', error);
    throw error;
  }
};


export const getCampaignStats = async (campaignId) => {
  try {
    const campaign = await Campaign.findById(campaignId)
      .select('stats status name created_at sent_at completed_at completion_duration_seconds')
      .lean();

    if (!campaign) {
      throw new Error('Campaign not found');
    }

    return campaign;

  } catch (error) {
    console.error('Error getting campaign stats:', error);
    throw error;
  }
};


export const updateCampaignStatsFromWhatsApp = async (waMessageId, status, timestamp = null, failureReason = null) => {
  try {
    const message = await Message.findOne({ wa_message_id: waMessageId })
      .select('_id metadata')
      .lean();

    if (!message) {
      console.log(`Message not found for WhatsApp ID: ${waMessageId}`);
      return null;
    }

    let internalStatus;
    switch (status) {
      case 'sent':
        internalStatus = 'sent';
        break;
      case 'delivered':
        internalStatus = 'delivered';
        break;
      case 'read':
        internalStatus = 'read';
        break;
      case 'failed':
        internalStatus = 'failed';
        break;
      default:
        console.log(`Unknown WhatsApp status: ${status}`);
        return null;
    }

    return await updateCampaignStatsFromMessage(
      message._id.toString(),
      internalStatus,
      timestamp,
      failureReason
    );

  } catch (error) {
    console.error('Error updating campaign stats from WhatsApp:', error);
    throw error;
  }
};

export default {
  updateCampaignStatsFromMessage,
  bulkUpdateCampaignStats,
  getCampaignStats,
  updateCampaignStatsFromWhatsApp
};
