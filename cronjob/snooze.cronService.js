import cron from 'node-cron';
import { 
    Contact, UserSetting, User,
    WhatsappPhoneNumber, WhatsappConnection, 
    TelegramConnection, FacebookConnection, InstagramConnection
} from '../models/index.js';

async function snoozeCronService(app) {
    cron.schedule('* * * * *', async () => {
        try {
            const contacts = await Contact.find({
                last_incoming_message_at: { $ne: null },
                chat_status: { $ne: 'resolved' },
                deleted_at: null,
                is_snoozed: true,
                $expr: {
                    $or: [
                        { $eq: ["$last_outgoing_message_at", null] },
                        { $gt: ["$last_incoming_message_at", "$last_outgoing_message_at"] }
                    ]
                }
            }).populate('user_id workspace_id assigned_to').lean();

            if (!contacts || contacts.length === 0) return;

            const activeConnectionsCache = {};

            for (const contact of contacts) {

                const source = contact.source || 'whatsapp';
                const workspaceId = contact.workspace_id ? contact.workspace_id._id?.toString() || contact.workspace_id.toString() : null;
                const userIdStr = contact.user_id._id.toString();
                const cacheKey = `${source}_${workspaceId}_${userIdStr}`;

                let isActiveConnection = activeConnectionsCache[cacheKey];
                if (isActiveConnection === undefined) {
                    try {
                        const query = workspaceId 
                            ? { $or: [{ workspace_id: workspaceId }, { user_id: userIdStr }] }
                            : { user_id: userIdStr };

                        if (source === 'whatsapp') {
                            const wPhone = await WhatsappPhoneNumber.findOne({ ...query, deleted_at: null }).lean();
                            isActiveConnection = !!wPhone;
                        } else if (source === 'baileys') {
                            const bConn = await WhatsappConnection.findOne({ ...query, status: 'connected' }).lean();
                            isActiveConnection = !!bConn;
                        } else if (source === 'telegram') {
                            const tConn = await TelegramConnection.findOne({ ...query, is_active: true }).lean();
                            isActiveConnection = !!tConn;
                        } else if (source === 'facebook') {
                            const fConn = await FacebookConnection.findOne({ ...query, is_active: true }).lean();
                            isActiveConnection = !!fConn;
                        } else if (source === 'instagram') {
                            const iConn = await InstagramConnection.findOne({ ...query, is_active: true }).lean();
                            isActiveConnection = !!iConn;
                        } else {
                            isActiveConnection = true;
                        }
                    } catch (err) {
                        console.error(`Error checking connection status for ${source}:`, err);
                        isActiveConnection = true;
                    }
                    activeConnectionsCache[cacheKey] = isActiveConnection;
                }

                if (!isActiveConnection) {
                    continue;
                }

                const snoozeTimeMs = (contact.snooze_time_minutes || 10) * 60 * 1000;
                const limit = contact.snooze_count_limit || 3;
                const currentCount = contact.snooze_count || 0;

                const timeSinceLastMessage = Date.now() - new Date(contact.last_incoming_message_at).getTime();
                const expectedTimePassed = (currentCount + 1) * snoozeTimeMs;

                if (timeSinceLastMessage >= expectedTimePassed) {
                    const newCount = currentCount + 1;
                    const isDanger = newCount > limit;
                    
                    const tenantUserId = contact.user_id._id;
                    const tenant = contact.user_id; 
                    const agentId = contact.assigned_to ? contact.assigned_to._id : null;
                    const agent = contact.assigned_to;

                    let alertMsg = `Reminder: ${contact.name} is waiting for a response.`;
                    if (isDanger) {
                        alertMsg = `Urgent: ${contact.name} has been waiting for over ${newCount * (contact.snooze_time_minutes || 10)} minutes.`;
                    }
                    
                    const io = app.get('io');
                    if (io) {
                        if (agentId) {
                            io.emit('snooze_reminder', {
                                contact_id: contact._id,
                                contact_name: contact.name,
                                contact_number: contact.phone_number || contact.telegram_chat_id || contact.facebook_page_scoped_id || contact.instagram_scoped_id,
                                user_id: agentId,
                                message: alertMsg,
                                isDanger,
                                snooze_count: newCount,
                                timestamp: new Date().toISOString()
                            });

                            if (isDanger && agentId.toString() !== tenantUserId.toString()) {
                                io.emit('snooze_reminder', {
                                    contact_id: contact._id,
                                    contact_name: contact.name,
                                    contact_number: contact.phone_number || contact.telegram_chat_id || contact.facebook_page_scoped_id || contact.instagram_scoped_id,
                                    user_id: tenantUserId,
                                    message: `🚨 Escalation: Agent ${agent.name} missed ${newCount} reminders to reply to ${contact.name}.`,
                                    isDanger,
                                    snooze_count: newCount,
                                    timestamp: new Date().toISOString()
                                });
                            }
                        } else {
                            io.emit('snooze_reminder', {
                                contact_id: contact._id,
                                contact_name: contact.name,
                                contact_number: contact.phone_number || contact.telegram_chat_id || contact.facebook_page_scoped_id || contact.instagram_scoped_id,
                                user_id: tenantUserId,
                                message: alertMsg,
                                isDanger,
                                snooze_count: newCount,
                                timestamp: new Date().toISOString()
                            });
                        }
                    }

                    await Contact.updateOne({ _id: contact._id }, { $inc: { snooze_count: 1 } });
                }
            }
        } catch (error) {
            console.error('Error in snooze auto-reminder cron job:', error);
        }
    });

    console.log('Snooze auto-reminder cron job scheduled.');
}

export default snoozeCronService;
