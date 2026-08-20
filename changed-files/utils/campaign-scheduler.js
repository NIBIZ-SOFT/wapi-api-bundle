import cronParser from 'cron-parser';
import moment from 'moment';
import Campaign from '../models/campaign.model.js';
import { processCampaignInBackground } from './campaign-processing.js';

/**
 * @param {string}   pattern       
 * @param {string}   cronExpression
 * @param {Date}     lastRunAt    
 * @returns {Date|null}          
 */
const computeNextRunDate = (pattern, cronExpression, lastRunAt) => {
  const now = new Date();
  let nextDate = null;

  try {
    if (pattern === 'daily') {
      nextDate = moment(lastRunAt).add(1, 'days').toDate();
      while (nextDate <= now) {
        nextDate = moment(nextDate).add(1, 'days').toDate();
      }

    } else if (pattern === 'weekly') {
      nextDate = moment(lastRunAt).add(1, 'weeks').toDate();
      while (nextDate <= now) {
        nextDate = moment(nextDate).add(1, 'weeks').toDate();
      }

    } else if (pattern === 'monthly') {
      nextDate = moment(lastRunAt).add(1, 'months').toDate();
      while (nextDate <= now) {
        nextDate = moment(nextDate).add(1, 'months').toDate();
      }

    } else if (pattern === 'custom_cron' && cronExpression) {
      const interval = cronParser.CronExpressionParser.parse(cronExpression, {
        currentDate: lastRunAt
      });
      nextDate = interval.next().toDate();

      while (nextDate <= now) {
        const catchUpInterval = cronParser.CronExpressionParser.parse(cronExpression, {
          currentDate: nextDate
        });
        nextDate = catchUpInterval.next().toDate();
      }
    }
  } catch (err) {
    console.error(`[Scheduler] computeNextRunDate error for pattern="${pattern}", cron="${cronExpression}":`, err.message);
    nextDate = null;
  }

  return nextDate;
};

class CampaignScheduler {
  constructor() {
    this.running = false;
    this.interval = null;
    this.checkIntervalMs = 60000;
    this._processing = false;
  }

  start() {
    if (this.running) {
      console.log('[Scheduler] Campaign scheduler is already running');
      return;
    }

    this.running = true;
    console.log('[Scheduler] Starting campaign scheduler...');

    this.checkAndProcessScheduledCampaigns();

    this.interval = setInterval(() => {
      this.checkAndProcessScheduledCampaigns();
    }, this.checkIntervalMs);
  }

  stop() {
    if (!this.running) {
      console.log('[Scheduler] Campaign scheduler is not running');
      return;
    }

    this.running = false;
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    console.log('[Scheduler] Campaign scheduler stopped');
  }

  async checkAndProcessScheduledCampaigns() {
    if (!this.running) return;


    if (this._processing) {
      console.log('[Scheduler] Previous check still running, skipping this tick');
      return;
    }

    this._processing = true;

    try {
      const scheduledCampaigns = await Campaign.getScheduledCampaigns();

      if (scheduledCampaigns.length > 0) {
        console.log(`[Scheduler] Found ${scheduledCampaigns.length} scheduled campaign(s) to process`);

        for (const campaign of scheduledCampaigns) {
          try {
            console.log(`[Scheduler] Processing scheduled campaign: ${campaign.name} (${campaign._id})`);
            processCampaignInBackground(campaign._id);
          } catch (error) {
            console.error(`[Scheduler] Error processing scheduled campaign ${campaign._id}:`, error);
            campaign.status = 'failed';
            campaign.error_log.push({
              timestamp: new Date(),
              error: `Scheduler error: ${error.message}`
            });
            await campaign.save();
          }
        }
      }

      const now = new Date();

      const recurringCampaigns = await Campaign.find({
        is_scheduled: true,
        is_recurring: true,
        is_published: true,
        status: { $in: ['scheduled', 'recurring'] },
        next_run_at: { $lte: now },
        deleted_at: null
      });

      if (recurringCampaigns.length > 0) {
        console.log(`[Scheduler] Found ${recurringCampaigns.length} recurring campaign(s) to process`);

        for (const parent of recurringCampaigns) {
          try {
            console.log(`[Scheduler] Processing recurring parent: ${parent.name} (${parent._id}), next_run_at=${parent.next_run_at}`);

            const lastRunAt = parent.next_run_at || now;
            const nextDate = computeNextRunDate(
              parent.recurring_pattern,
              parent.cron_expression,
              lastRunAt
            );

            const childData = parent.toObject();

            delete childData._id;
            delete childData.createdAt;
            delete childData.updatedAt;
            delete childData.__v;
            delete childData.error_log;
            delete childData.deleted_at;
            delete childData.created_at;
            delete childData.updated_at;

            childData.is_recurring = false;
            childData.recurring_pattern = null;
            childData.cron_expression = null;
            childData.recurring_end_date = null;
            childData.next_run_at = null;
            childData.parent_recurring_id = parent._id;
            childData.status = 'draft';
            childData.sent_at = null;
            childData.is_paused = false;
            childData.stats = {
              total_recipients: parent.stats?.total_recipients || 0,
              pending_count: parent.stats?.total_recipients || 0,
              sent_count: 0,
              failed_count: 0,
              delivered_count: 0,
              read_count: 0
            };

            const parentDoc = await Campaign.findById(parent._id).select('recipients').lean();
            if (parentDoc && parentDoc.recipients) {
              childData.recipients = parentDoc.recipients.map(r => ({
                contact_id: r.contact_id,
                phone_number: r.phone_number,
                status: 'pending',
                message_id: null,
                sent_at: null,
                delivered_at: null,
                read_at: null,
                failed_at: null,
                failure_reason: null
              }));
            }

            const childCampaign = await Campaign.create(childData);
            console.log(`[Scheduler] Created child campaign ${childCampaign._id} for parent ${parent._id}`);
            processCampaignInBackground(childCampaign._id);

            if (nextDate && (!parent.recurring_end_date || nextDate <= new Date(parent.recurring_end_date))) {
              parent.next_run_at = nextDate;
              parent.status = 'recurring';
              console.log(`[Scheduler] Parent ${parent._id} next run scheduled at ${nextDate.toISOString()}`);
            } else {
              parent.is_scheduled = false;
              parent.status = 'completed';
              parent.next_run_at = null;
              console.log(`[Scheduler] Parent ${parent._id} has no more valid runs, marking as completed`);
            }

            await parent.save();

          } catch (error) {
            console.error(`[Scheduler] Error processing recurring campaign ${parent._id}:`, error);
            parent.status = 'failed';
            parent.error_log.push({
              timestamp: new Date(),
              error: `Recurring scheduler error: ${error.message}`
            });
            await parent.save();
          }
        }
      }

    } catch (error) {
      console.error('[Scheduler] Unexpected error in campaign scheduler:', error);
    } finally {
      this._processing = false;
    }
  }

  getStatus() {
    return {
      running: this.running,
      processing: this._processing,
      checkIntervalMs: this.checkIntervalMs,
      nextCheck: this.running ? new Date(Date.now() + this.checkIntervalMs) : null
    };
  }
}

export default new CampaignScheduler();
