import { AutomationFlow, AutomationExecution, Contact, EcommerceOrder, Message, WhatsappPhoneNumber, Template, Tag, ContactTag, ChatAssignment, Chatbot, ReplyMaterial, EcommerceProduct } from '../models/index.js';
import unifiedWhatsAppService from '../services/whatsapp/unified-whatsapp.service.js';
import BusinessAPIProvider from '../services/whatsapp/providers/business-api.provider.js';

const businessApiProvider = new BusinessAPIProvider();
import { getSheetsClient, getCalendarClient, getFormsClient, handleGoogleApiError } from './google-api-helper.js';
import { handleSequenceReply } from './automated-response.service.js';
import { PROVIDER_TYPES } from '../services/whatsapp/unified-whatsapp.service.js';
import appointmentService from '../services/appointment.service.js';
import automationCache from './automation-cache.js';
import { v4 as uuidv4 } from 'uuid';

class AutomationEngine {
  constructor() {
    this.runningExecutions = new Map();
    this.eventListeners = new Map();
    this.initializeEventListeners();
    this.startTimeoutWorker();
  }

  startTimeoutWorker() {
    setInterval(() => {
      this.processTimeouts().catch(err => console.error('Error in timeout worker:', err));
    }, 60 * 1000);
  }

  async processTimeouts() {
    try {
      const timedOutExecutions = await AutomationExecution.find({
        status: 'waiting',
        timeout_at: { $lte: new Date() }
      });

      if (timedOutExecutions.length === 0) return;

      console.log(`[Flow Engine Worker] Found ${timedOutExecutions.length} timed-out executions.`);

      for (const execution of timedOutExecutions) {
        try {
          const flow = await AutomationFlow.findById(execution.flow_id);
          if (!flow || !flow.is_active || flow.deleted_at) {
            await AutomationExecution.findByIdAndUpdate(execution._id, { status: 'cancelled', completed_at: new Date() });
            continue;
          }

          if (flow.is_paused) {
            continue;
          }

          const waitingNode = flow.nodes.find(n => n.id === execution.waiting_for_node_id);
          if (!waitingNode) {
            await AutomationExecution.findByIdAndUpdate(execution._id, { status: 'failed', error: 'Waiting node not found', completed_at: new Date() });
            continue;
          }

          let nextNodeId = null;
          let currentData = { ...execution.input_data };
          currentData.executionId = execution._id;
          const fallbackRoute = waitingNode.parameters?.fallback_route;

          if (fallbackRoute) {
            const connection = flow.connections.find(c => c.source === waitingNode.id && c.sourceHandle === fallbackRoute);
            if (connection) {
              nextNodeId = connection.target;
              currentData.__nextHandle = fallbackRoute;
            }
          }

          await AutomationExecution.findByIdAndUpdate(execution._id, {
            status: 'running',
            contact_identifier: null
          });

          if (!nextNodeId) {
            await AutomationExecution.findByIdAndUpdate(execution._id, { status: 'success', completed_at: new Date() });
            continue;
          }

          const nextNode = flow.nodes.find(n => n.id === nextNodeId);
          if (nextNode) {
            console.log(`[Flow Engine Worker] Resuming execution ${execution._id} via timeout route to ${nextNodeId}`);

            const executionLog = execution.execution_log || [];
            const executionId = uuidv4();
            this.runningExecutions.set(executionId, execution._id);

            const startTime = Date.now();
            const nodeResult = await this.executeNode(nextNode, flow, currentData, executionLog);

            if (nodeResult.status === 'waiting') {
              this.runningExecutions.delete(executionId);
              continue;
            }

            if (nodeResult.success) {
              const updatedData = { ...currentData, ...nodeResult.output };
              const connResult = await this.processConnectedNodes(flow, nextNode, updatedData, executionLog, currentData);
              if (connResult && connResult.status === 'waiting') {
                this.runningExecutions.delete(executionId);
                continue;
              }
              if (connResult && connResult.success) {
                currentData = connResult.output;
              } else {
                currentData = updatedData;
              }
            }

            await AutomationExecution.findByIdAndUpdate(execution._id, {
              status: nodeResult.success ? 'success' : 'failed',
              output_data: currentData,
              execution_time: Date.now() - startTime,
              completed_at: new Date(),
              execution_log: executionLog
            });

            await this.updateFlowStatistics(flow._id, nodeResult.success);
            this.runningExecutions.delete(executionId);
          } else {
            await AutomationExecution.findByIdAndUpdate(execution._id, { status: 'success', completed_at: new Date() });
          }
        } catch (execErr) {
          console.error(`[Flow Engine Worker] Error processing timeout for execution ${execution._id}:`, execErr);
          await AutomationExecution.findByIdAndUpdate(execution._id, { status: 'failed', error: execErr.message, completed_at: new Date() });
        }
      }
    } catch (err) {
      console.error('[Flow Engine Worker] Core loop error:', err);
    }
  }


  initializeEventListeners() {
    this.eventListeners.set('message_received', this.handleMessageReceived.bind(this));

    this.eventListeners.set('contact_joined', this.handleContactJoined.bind(this));
    this.eventListeners.set('status_update', this.handleStatusUpdate.bind(this));
    this.eventListeners.set('order_received', this.handleOrderReceived.bind(this));

    console.log('Automation engine event listeners initialized:', Array.from(this.eventListeners.keys()));
  }

  async handleOrderReceived(eventData) {
    try {
      console.log("=====================handleOrderReceived called", eventData);
      const { userId } = eventData;

      let contact = null;
      try {
        if (eventData.contactId) {
          contact = await Contact.findOne({
            _id: eventData.contactId,
            created_by: userId,
            deleted_at: null
          }).lean();
        }
      } catch (contactErr) {
        console.warn('Failed to load contact for order_received:', contactErr.message);
      }

      const triggers = await automationCache.getUserActiveFlows(userId);
      console.log(`Found ${triggers.length} triggers for user ${userId}`);

      const incomingWorkspaceId = eventData.workspaceId ? eventData.workspaceId.toString() : null;
      console.log(`Incoming workspaceId: ${incomingWorkspaceId}`);

      const workspaceTriggers = triggers.filter(t => {
        const triggerWorkspaceId = t.workspace_id ? t.workspace_id.toString() : null;
        return !triggerWorkspaceId || triggerWorkspaceId === incomingWorkspaceId;
      });

      const orderTriggers = workspaceTriggers.filter(t => t.event_type === 'order_received');
      console.log(`Found ${orderTriggers.length} order received triggers for workspace`);

      for (const trigger of orderTriggers) {
        let flow = automationCache.getFlow(trigger.flow_id.toString());
        if (!flow) {
          flow = await AutomationFlow.findById(trigger.flow_id).populate('user_id');
          if (flow) {
            automationCache.setFlow(trigger.flow_id.toString(), flow);
            console.log(`Loaded flow from DB and cached: ${trigger.flow_id}`);
          }
        }

        if (flow && flow.is_active && !flow.is_paused && !flow.deleted_at) {
          const shouldExecute = this.checkOrderTriggerConditions(flow, eventData);
          console.log(`Should execute order flow: ${shouldExecute}`);
          if (shouldExecute) {
            await this.executeFlow(flow, {
              event_type: 'order_received',
              ...eventData,
              contact,
              timestamp: new Date()
            });
          }
        }
      }
    } catch (error) {
      console.error('Error handling order received event:', error);
    }
  }

  checkOrderTriggerConditions(flow, eventData) {
    const triggers = flow.triggers.filter(t => t.event_type === 'order_received');

    const dataObject = {
      eventType: "orderReceived",
      order_id: eventData.order_id,
      wa_order_id: eventData.wa_order_id,
      wa_message_id: eventData.wa_message_id,
      total_price: eventData.total_price,
      currency: eventData.currency,
      items_count: eventData.items_count,
      senderNumber: eventData.senderNumber,
      recipientNumber: eventData.recipientNumber,
      contactId: eventData.contactId,
      userId: eventData.userId,
      whatsappPhoneNumberId: eventData.whatsappPhoneNumberId
    };

    for (const trigger of triggers) {
      const conditions = trigger.conditions || {};
      if (Object.keys(conditions).length === 0) {
        return true;
      }

      const result = this.evaluateCondition(conditions, dataObject);
      if (result) return true;
    }

    return false;
  }


  async handleMessageReceived(eventData) {
    try {
      console.log("=====================handleMessageReceived called", eventData);
      const { message, senderNumber, recipientNumber, userId, messageType, interactive_id } = eventData;
      const effectiveMessage = interactive_id || message;
      const normalizedMessagePayload = this.normalizeMessagePayload(effectiveMessage);

      let contact = null;
      try {
        if (eventData.contactId) {
          contact = await Contact.findOne({
            _id: eventData.contactId,
            created_by: userId,
            deleted_at: null
          }).lean();
        } else if (senderNumber) {
          contact = await Contact.findOne({
            $or: [
              { phone_number: senderNumber },
              { telegram_chat_id: senderNumber },
              { facebook_page_scoped_id: senderNumber },
              { instagram_scoped_id: senderNumber }
            ],
            created_by: userId,
            deleted_at: null
          }).lean();
        }
      } catch (contactErr) {
        console.warn('Failed to load contact for message_received:', contactErr.message);
      }

      const triggers = await automationCache.getUserActiveFlows(userId);
      console.log(`Found ${triggers.length} triggers for user ${userId}`);

      const incomingWorkspaceId = eventData.workspaceId ? eventData.workspaceId.toString() : null;
      console.log(`Incoming workspaceId: ${incomingWorkspaceId}`);

      const workspaceTriggers = triggers.filter(t => {
        const triggerWorkspaceId = t.workspace_id ? t.workspace_id.toString() : null;
        return !triggerWorkspaceId || triggerWorkspaceId === incomingWorkspaceId;
      });

      const messageTriggers = workspaceTriggers.filter((t, i, arr) => t.event_type === 'message_received' && arr.findIndex(tt => String(tt.flow_id) === String(t.flow_id) && tt.event_type === 'message_received') === i);
      console.log(`Found ${messageTriggers.length} message received triggers for workspace`);

      const waitingQuery = {
        contact_identifier: senderNumber,
        status: 'waiting',
        user_id: userId
      };
      if (incomingWorkspaceId) {
        waitingQuery.workspace_id = incomingWorkspaceId;
      }

      const waitingExecution = await AutomationExecution.findOne(waitingQuery).sort({ updated_at: -1 });

      let incomingPlatform = eventData.platform || 'whatsapp';
      if (['baileys', 'business_api', 'cloud_api'].includes(incomingPlatform)) {
        incomingPlatform = 'whatsapp';
      }

      if (waitingExecution) {
        console.log(`Found waiting execution ${waitingExecution._id} for ${senderNumber}. Resuming...`);
        const flow = await AutomationFlow.findById(waitingExecution.flow_id).populate('user_id');
        if (flow && flow.is_active && !flow.is_paused && !flow.deleted_at) {
          const flowPlatform = flow.platform || 'whatsapp';
          if (flowPlatform === 'all' || flowPlatform === incomingPlatform) {
            const resumeResult = await this.resumeExecution(flow, waitingExecution, eventData, normalizedMessagePayload);
            if (resumeResult && resumeResult.notMatched) {
              console.log('[handleMessageReceived] resumeExecution did not match waiting node. Falling back to trigger evaluation.');
            } else {
              return resumeResult;
            }
          } else {
            console.log(`Skipping waiting execution flow ${flow.name} because platform ${flowPlatform} does not match incoming ${incomingPlatform}`);
          }
        }
      }

      let bestFlowToExecute = null;
      let bestFlowWeight = 999;

      for (const trigger of messageTriggers) {
        console.log(`Processing trigger:`, trigger);
        let flow = automationCache.getFlow(trigger.flow_id.toString());
        if (!flow) {
          flow = await AutomationFlow.findById(trigger.flow_id).populate('user_id');
          if (flow) {
            automationCache.setFlow(trigger.flow_id.toString(), flow);
            console.log(`Loaded flow from DB and cached: ${trigger.flow_id}`);
          }
        }

        if (flow && flow.is_active && !flow.is_paused && !flow.deleted_at) {
          const flowPlatform = flow.platform || 'whatsapp';
          if (flowPlatform !== 'all' && flowPlatform !== incomingPlatform) {
            console.log(`Skipping flow ${flow.name} (${flowPlatform}) for incoming message on platform ${incomingPlatform}`);
            continue;
          }

          console.log(`Checking conditions for flow:`, flow.name);
          const { match, weight } = this.checkMessageTriggerConditions(flow, effectiveMessage, senderNumber, recipientNumber, messageType, null, eventData, normalizedMessagePayload);
          console.log(`Should execute flow: ${match} (weight: ${weight})`);
          if (match) {
            if (weight < bestFlowWeight) {
              bestFlowToExecute = flow;
              bestFlowWeight = weight;
            }
          } else {
            console.log(`Flow conditions not met for: ${flow.name}`);
          }
        } else {
          console.log(`Flow not active or deleted:`, flow?.name);
        }
      }

      if (bestFlowToExecute) {
        console.log(`Executing flow: ${bestFlowToExecute.name} for message: ${effectiveMessage}`);
        await this.executeFlow(bestFlowToExecute, {
          event_type: 'message_received',
          message: effectiveMessage,
          interactive_id,
          messagePayload: normalizedMessagePayload,
          senderNumber,
          recipientNumber,
          userId,
          messageType,
          contactId: eventData.contactId || contact?._id?.toString() || null,
          contact,
          whatsappPhoneNumberId: eventData.whatsappPhoneNumberId,
          timestamp: new Date()
        });
      }
    } catch (error) {
      console.error('Error handling message received event:', error);
    }
  }


  checkMessageTriggerConditions(flow, message, senderNumber, recipientNumber, messageType, messageId, eventData = null, messagePayload = null) {
    console.log(`Checking conditions for flow: ${flow.name}`, { message, senderNumber, recipientNumber, messageType });
    const triggers = flow.triggers.filter(t => t.event_type === 'message_received');
    console.log(`Found ${triggers.length} message received triggers in flow`);

    const dataObject = {
      message: message || messageId,
      interactive_id: eventData?.interactive_id || null,
      messagePayload: messagePayload || this.normalizeMessagePayload(message || messageId),
      senderNumber,
      recipientNumber,
      messageType,
      eventType: "messageReceived",
      event_type: "message_received"
    };

    if (eventData && eventData.whatsappPhoneNumberId) {
      dataObject.whatsappPhoneNumberId = eventData.whatsappPhoneNumberId;
    }

    let matched = false;
    let bestWeight = 999;

    for (const trigger of triggers) {
      const conditions = trigger.conditions || {};
      console.log(`Checking conditions:`, conditions);

      let weight = 4;
      if (conditions.operator === 'equals') weight = 1;
      else if (conditions.operator === 'starts_with') weight = 2;
      else if (conditions.operator === 'contains_any') weight = 3;

      if (Object.keys(conditions).length === 0 || (conditions.field === 'event_type' && conditions.value === 'message_received')) {
        const triggerNode = flow.nodes?.find(n => n.type === 'trigger');
        if (triggerNode) {
          const triggerType = triggerNode.parameters?.triggerType;
          if (triggerType && triggerType !== 'any message') {
            console.log(`Flow ${flow.name} triggerType is ${triggerType} but conditions are empty. Skipping.`);
            continue;
          }
          if (!triggerType) {
            console.log(`Flow ${flow.name} trigger node has no triggerType specified (sub-flow or unconfigured). Skipping.`);
            continue;
          }
        } else {
          console.log(`Flow ${flow.name} has no trigger node. Skipping direct message trigger.`);
          continue;
        }

        console.log('No specific conditions specified, triggering flow for all messages (weight 4)');
        matched = true;
        if (weight < bestWeight) bestWeight = weight;
        continue;
      }

      const result = this.evaluateCondition(conditions, dataObject);

      console.log(`Condition evaluation result: ${result}`);
      if (result) {
        console.log(`All conditions met for flow: ${flow.name} with weight: ${weight}`);
        matched = true;
        if (weight < bestWeight) bestWeight = weight;
      }
    }

    if (!matched) {
      console.log(`No matching triggers found for flow: ${flow.name}`);
    }
    return { match: matched, weight: bestWeight };
  }


  async handleContactJoined(eventData) {
    console.log('Contact joined event:', eventData);
  }


  async handleStatusUpdate(eventData) {
    console.log('Status update event:', eventData);
  }


  async executeFlow(flow, inputData) {
    const executionId = uuidv4();
    try {
      const execution = await AutomationExecution.create({
        flow_id: flow._id,
        user_id: flow.user_id._id || flow.user_id,
        workspace_id: flow.workspace_id || null,
        status: 'running',
        input_data: inputData
      });

      this.runningExecutions.set(executionId, execution._id);

      const result = await this.processWorkflow(flow, execution, { ...inputData, flow_id: flow._id, executionId: execution._id });

      if (result.status === 'waiting') {
        this.runningExecutions.delete(executionId);
        return result;
      }

      await AutomationExecution.findByIdAndUpdate(execution._id, {
        status: result.success ? 'success' : 'failed',
        output_data: result.output,
        execution_time: result.executionTime,
        completed_at: new Date(),
        execution_log: result.executionLog
      });

      await this.updateFlowStatistics(flow._id, result.success);

      this.runningExecutions.delete(executionId);
      return result;
    } catch (error) {
      console.error('Error executing automation flow:', error);

      if (executionId) {
        await AutomationExecution.findByIdAndUpdate(
          this.runningExecutions.get(executionId),
          {
            status: 'failed',
            error: error.message,
            completed_at: new Date()
          }
        );
        this.runningExecutions.delete(executionId);
      }

      throw error;
    }
  }


  async processWorkflow(flow, execution, inputData) {
    const startTime = Date.now();
    const executionLog = [];
    let currentData = { ...inputData };

    const startNodes = this.getStartNodes(flow, inputData);
    for (const node of startNodes) {

      const nodeResult = await this.executeNode(node, flow, currentData, executionLog);
      if (nodeResult.status === 'waiting') {
        return { success: true, status: 'waiting', output: currentData, executionLog };
      }
      if (nodeResult.success) {
        currentData = {
          ...currentData,
          ...nodeResult.output,
          userId: currentData.userId || inputData.userId || inputData.user_id
        };

        const workflowResult = await this.processConnectedNodes(flow, node, currentData, executionLog, inputData);
        if (workflowResult && workflowResult.status === 'waiting') {
          return workflowResult;
        }
        if (workflowResult && workflowResult.success) {
          currentData = workflowResult.output;
        }
      }
    }

    const executionTime = Date.now() - startTime;

    return {
      success: true,
      output: currentData,
      executionTime,
      executionLog
    };
  }


  getStartNodes(flow, inputData = null) {
    const connectedTargets = new Set();
    flow.connections.forEach(conn => {
      connectedTargets.add(conn.target);
    });

    const triggerNodes = flow.nodes.filter(node => node.type === 'trigger');

    let startNodes = [];
    if (triggerNodes.length > 0) {
      startNodes = triggerNodes.filter(node => !connectedTargets.has(node.id));
    } else {
      startNodes = flow.nodes.filter(node => !connectedTargets.has(node.id) && node.type === 'trigger');
    }

    if (inputData && startNodes.length > 0) {
      const matchedNodes = [];
      for (const node of startNodes) {
        const { triggerType, keywords, conditions } = node.parameters || {};

        if (triggerType) {
          let condObj = null;
          if (triggerType === 'order received') {
            condObj = { field: 'event_type', operator: 'equals', value: 'order_received' };
          } else if (triggerType === 'any message') {
            condObj = { field: 'event_type', operator: 'equals', value: 'message_received' };
          } else {
            const operatorMap = {
              "contains keyword": "contains_any",
              "on exact match": "equals",
              "starts with": "starts_with"
            };
            condObj = {
              field: 'message',
              operator: operatorMap[triggerType] || 'contains_any',
              value: keywords || []
            };
          }

          const dataObject = {
            message: inputData.message || inputData.messagePayload,
            interactive_id: inputData.interactive_id,
            senderNumber: inputData.senderNumber,
            recipientNumber: inputData.recipientNumber,
            messageType: inputData.messageType,
            eventType: inputData.event_type || 'message_received',
            event_type: inputData.event_type || 'message_received'
          };

          let matched = this.evaluateCondition(condObj, dataObject);
          if (!matched && flow.triggers) {
            for (const t of flow.triggers) {
              if (t.event_type === 'message_received' && t.conditions) {
                if (this.evaluateCondition(t.conditions, dataObject)) {
                  matched = true;
                  break;
                }
              }
            }
          }
          if (matched) {
            matchedNodes.push(node);
          }
        } else if (Array.isArray(conditions) && conditions.length > 0) {
          // For backend generated triggers
          const dataObject = {
            message: inputData.message || inputData.messagePayload,
            interactive_id: inputData.interactive_id,
            senderNumber: inputData.senderNumber,
            recipientNumber: inputData.recipientNumber,
            messageType: inputData.messageType,
            eventType: inputData.event_type || 'message_received',
            event_type: inputData.event_type || 'message_received'
          };
          let matched = false;
          for (const cond of conditions) {
            if (this.evaluateCondition(cond, dataObject)) {
              matched = true;
              break;
            }
          }
          if (matched) matchedNodes.push(node);
        } else {
          // If no specific trigger conditions, assume it matches
          matchedNodes.push(node);
        }
      }
      return matchedNodes;
    }

    return startNodes;
  }

  async processConnectedNodes(flow, currentNode, currentData, executionLog, originalInputData = {}) {
    const nextHandle = currentData.__nextHandle || null;
    if (nextHandle) {
      delete currentData.__nextHandle;
    }

    if (!nextHandle) {
      const hasInteractiveBranches = flow.connections.some(c =>
        (c.source === currentNode.id && c.sourceHandle && (c.sourceHandle.startsWith('src-btn-') || c.sourceHandle.startsWith('src-item-'))) ||
        (c.target && (c.target.startsWith(`cond-btn-___${currentNode.id}___`) || c.target.startsWith(`cond-list-___${currentNode.id}___`)))
      );

      if (hasInteractiveBranches) {
        const executionId = currentData.executionId;
        if (executionId) {
          // Find a fallback non-interactive next node (e.g. condition/action connected without a btn handle)
          // so resumeExecution can continue even if button index matching fails.
          const fallbackConnection = flow.connections.find(c =>
            c.source === currentNode.id &&
            (!c.sourceHandle || (!c.sourceHandle.startsWith('src-btn-') && !c.sourceHandle.startsWith('src-item-')))
          );
          const fallbackNextNodeId = fallbackConnection ? fallbackConnection.target : null;

          await AutomationExecution.findByIdAndUpdate(executionId, {
            status: 'waiting',
            waiting_for_node_id: currentNode.id,
            next_node_id: fallbackNextNodeId,
            contact_identifier: currentData.senderNumber || currentData.phone_number || currentData.recipientNumber,
            input_data: currentData
          });
          return { success: true, status: 'waiting', output: currentData, executionLog };
        }
      }
    }

    // Auto-wait: if the current node is send_message and the next node is a condition,
    // pause and wait for user reply before evaluating the condition.
    // Skip during resumed executions (is_resuming=true) so Logic Control branch
    // Send Messages can flow straight through to ask_and_wait nodes.
    const CONDITION_TYPES = ['advanced_condition', 'condition'];
    if (!currentData.is_test && !currentData.is_resuming && currentNode.type === 'send_message') {
      const nextNodes = this.getConnectedNodes(flow, currentNode.id, nextHandle);
      const conditionNext = nextNodes.find(n => CONDITION_TYPES.includes(n.type));
      if (conditionNext) {
        const executionId = currentData.executionId;
        if (executionId) {
          console.log(`[Flow Engine] Auto-wait: send_message → ${conditionNext.type}. Pausing for user reply before evaluating condition.`);
          await AutomationExecution.findByIdAndUpdate(executionId, {
            status: 'waiting',
            waiting_for_node_id: currentNode.id,
            next_node_id: conditionNext.id,
            contact_identifier: currentData.senderNumber || currentData.phone_number || currentData.recipientNumber,
            input_data: currentData
          });
          return { success: true, status: 'waiting', output: currentData, executionLog };
        }
      }
    }

    const connectedNodes = this.getConnectedNodes(flow, currentNode.id, nextHandle);
    console.log(`[Flow Chain] Node "${currentNode.name || currentNode.id}" (${currentNode.type}) → next: [${connectedNodes.map(n => `"${n.name || n.id}"(${n.type})`).join(', ')}] handle="${nextHandle || 'none'}"`);

    for (const node of connectedNodes) {
      const nodeResult = await this.executeNode(node, flow, currentData, executionLog);
      if (nodeResult.status === 'waiting') {
        console.log(`[Flow Chain] Node "${node.name || node.id}" (${node.type}) returned WAITING — propagating up.`);
        return { success: true, status: 'waiting', output: currentData, executionLog };
      }
      if (nodeResult.success) {
        const updatedData = {
          ...currentData,
          ...nodeResult.output,
          userId: currentData.userId || originalInputData.userId || originalInputData.user_id
        };

        currentData = updatedData;

        const result = await this.processConnectedNodes(flow, node, updatedData, executionLog, originalInputData);
        if (result && result.status === 'waiting') {
          return result;
        }
        if (result && result.success) {
          currentData = result.output;
        }
      } else {
        console.log(`[Flow Chain] Node "${node.name || node.id}" (${node.type}) FAILED: ${nodeResult.error}`);
      }
    }
    return { success: true, output: currentData };
  }


  getConnectedNodes(flow, nodeId, sourceHandle = null) {
    const connections = flow.connections.filter(conn => {
      if (conn.source !== nodeId) return false;
      if (sourceHandle && conn.sourceHandle) {
        return conn.sourceHandle === sourceHandle;
      }
      // If no specific sourceHandle is requested, exclude interactive branches
      if (!sourceHandle && conn.sourceHandle && (conn.sourceHandle.startsWith('src-btn-') || conn.sourceHandle.startsWith('src-item-'))) {
        return false;
      }
      return !sourceHandle;
    });

    const connectedIds = connections.map(conn => conn.target);

    return flow.nodes.filter(node => connectedIds.includes(node.id));
  }


  async executeNode(node, flow, inputData, executionLog) {
    const startTime = Date.now();
    let result = { success: false, output: {} };

    try {
      console.log(`\x1b[36m[Flow Engine]\x1b[0m >>> Executing Node: "${node.name || node.id}" (Type: ${node.type})`);

      const nodeLog = {
        node_id: node.id,
        node_type: node.type,
        status: 'running',
        start_time: new Date(),
        input: inputData
      };

      switch (node.type) {
        case 'trigger':
          result = await this.executeTriggerNode(node, inputData);
          break;
        case 'condition':
          result = await this.executeConditionNode(node, inputData);
          break;
        case 'advanced_condition':
          result = await this.executeAdvancedConditionNode(node, flow, inputData);
          break;
        case 'action':
          result = await this.executeActionNode(node, inputData);
          break;
        case 'delay':
          result = await this.executeDelayNode(node, inputData);
          break;
        case 'filter':
          result = await this.executeFilterNode(node, inputData);
          break;
        case 'transform':
          result = await this.executeTransformNode(node, inputData);
          break;
        case 'webhook':
          result = await this.executeWebhookNode(node, inputData);
          break;
        case 'ai_response':
          result = await this.executeAIResponseNode(node, inputData);
          break;
        case 'send_message':
          result = await this.executeSendMessageNode(node, inputData);
          break;
        case 'send_template':
          result = await this.executeSendTemplateNode(node, inputData);
          break;
        case 'add_tag':
          result = await this.executeAddTagNode(node, inputData);
          break;
        case 'remove_tag':
          result = await this.executeRemoveTagNode(node, inputData);
          break;
        case 'assign_agent':
          result = await this.executeAssignAgentNode(node, inputData);
          break;
        case 'assign_random_agent':
          result = await this.executeAssignRandomAgentNode(node, inputData);
          break;
        case 'cta_button':
          result = await this.executeSendCtaNode(node, inputData);
          break;
        case 'assign_chatbot':
          result = await this.executeAssignChatbotNode(node, inputData);
          break;
        case 'save_to_google_sheet':
          result = await this.executeSaveToGoogleSheetNode(node, inputData);
          break;

        case 'send_google_form':
          result = await this.executeSendGoogleFormNode(node, inputData);
          break;
        case 'create_calendar_event':
          result = await this.executeCreateCalendarEventNode(node, inputData);
          break;
        case 'create_google_meet':
          result = await this.executeCreateGoogleMeetNode(node, inputData);
          break;
        case 'update_contact':
          result = await this.executeUpdateContactNode(node, inputData);
          break;
        case 'add_to_segment':
          result = await this.executeAddToSegmentNode(node, inputData);
          break;
        case 'appointment_flow':
          result = await this.executeAppointmentFlowNode(node, flow, inputData, executionLog);
          break;
        case 'send_sequence':
          result = await this.executeSendSequenceNode(node, inputData);
          break;
        case 'run_sub_flow':
          result = await this.executeRunSubFlowNode(node, flow, inputData, executionLog);
          break;
        case 'wait_for_reply':
          result = await this.executeWaitForReplyNode(node, flow, inputData, executionLog);
          break;
        case 'ask_and_wait':
          result = await this.executeAskAndWaitNode(node, flow, inputData, executionLog);
          break;
        case 'form_flow':
          result = await this.executeFormFlowNode(node, inputData);
          break;
        case 'response_saver':
          result = await this.executeResponseSaverNode(node, inputData);
          break;
        case 'api':
          result = await this.executeApiNode(node, inputData);
          break;
        case 'send_product':
        case 'send_catalog':
        case 'send_shopify':
          result = await this.executeSendProductNode(node, inputData);
          break;
        case 'move_to_pipeline_stage':
          result = await this.executeMoveToPipelineStageNode(node, inputData);
          break;
        case 'custom':
          result = await this.executeCustomNode(node, inputData);
          break;
        default:
          throw new Error(`Unknown node type: ${node.type}`);
      }

      nodeLog.status = result.success ? 'success' : 'failed';
      nodeLog.output = result.output;
      nodeLog.end_time = new Date();
      nodeLog.error = result.error || null;

      executionLog.push(nodeLog);

      if (result.success) {
        console.log(`\x1b[32m[Flow Engine]\x1b[0m Node "${node.name || node.id}" \x1b[32mSUCCESS\x1b[0m. Output keys: ${Object.keys(result.output || {})}`);
      } else {
        console.error(`\x1b[31m[Flow Engine]\x1b[0m Node "${node.name || node.id}" \x1b[31mFAILED\x1b[0m. Error: ${result.error}`);
      }

      return result;
    } catch (error) {
      const nodeLog = {
        node_id: node.id,
        node_type: node.type,
        status: 'failed',
        start_time: new Date(),
        end_time: new Date(),
        input: inputData,
        output: {},
        error: error.message
      };
      executionLog.push(nodeLog);

      return { success: false, output: {}, error: error.message };
    }
  }


  async executeTriggerNode(node, inputData) {
    return { success: true, output: inputData };
  }


  async executeConditionNode(node, inputData) {
    const { condition, conditions, no_match_handle } = node.parameters || {};

    if (Array.isArray(conditions) && conditions.length > 0) {
      try {
        let matchedHandle = null;
        let matchedConditionId = null;

        for (const cond of conditions) {
          const { id, field, operator, value, sourceHandle } = cond;
          const condObj = { field, operator, value };
          const result = this.evaluateCondition(condObj, inputData);
          if (result) {
            matchedHandle = sourceHandle || id || null;
            matchedConditionId = id || null;
            break;
          }
        }

        const output = {
          ...inputData,
          conditionMatched: !!matchedHandle,
          matchedConditionId
        };

        if (matchedHandle) {
          output.__nextHandle = matchedHandle;
        } else if (no_match_handle) {
          output.__nextHandle = no_match_handle;
        }

        return { success: true, output };
      } catch (error) {
        return { success: false, output: {}, error: error.message };
      }
    }

    if (!condition) {
      return { success: true, output: inputData };
    }

    try {
      const result = this.evaluateCondition(condition, inputData);
      return { success: result, output: { ...inputData, conditionResult: result } };
    } catch (error) {
      return { success: false, output: {}, error: error.message };
    }
  }


  async executeAdvancedConditionNode(node, flow, inputData) {
    const { match_type = 'AND', rules = [] } = node.parameters || {};

    try {
      let trueHandle = 'true';
      let falseHandle = 'false';

      if (flow && flow.connections) {
        const connections = flow.connections.filter(c => c.source === node.id);
        if (connections.some(c => c.sourceHandle === 'if-true')) {
          trueHandle = 'if-true';
          falseHandle = 'if-false';
        } else if (connections.some(c => c.sourceHandle === 'condition-true')) {
          trueHandle = 'condition-true';
          falseHandle = 'condition-false';
        }
      }

      if (!Array.isArray(rules) || rules.length === 0) {
        const output = { ...inputData, conditionMatched: true };
        output.__nextHandle = trueHandle;
        return { success: true, output };
      }

      let isMatch = match_type === 'OR' ? false : true;

      for (const rule of rules) {
        const condObj = { field: rule.field, operator: rule.operator, value: rule.value };
        const result = this.evaluateCondition(condObj, inputData);

        if (match_type === 'OR') {
          if (result) {
            isMatch = true;
            break;
          }
        } else {
          if (!result) {
            isMatch = false;
            break;
          }
        }
      }

      const output = {
        ...inputData,
        conditionMatched: isMatch
      };

      output.__nextHandle = isMatch ? trueHandle : falseHandle;

      return { success: true, output };
    } catch (error) {
      return { success: false, output: {}, error: error.message };
    }
  }

  evaluateCondition(condition, data) {
    let { field, operator, value } = condition;

    if (!field && (operator || value !== undefined)) {
      field = 'message';
    }

    if (!field || !operator) {
      return false;
    }

    if (value === undefined && !['exists', 'is_empty', 'is_not_empty'].includes(operator)) {
      return false;
    }

    const matched = this.evaluateSingleValue(field, operator, value, this.getFieldValue(field, data));
    if (matched) return true;

    // Fallback: If it's a message field and textContent is available, try matching against textContent
    if ((field === 'message' || field === 'message.body' || (typeof field === 'string' && field.startsWith('message.'))) && data.textContent) {
      return this.evaluateSingleValue(field, operator, value, data.textContent);
    }

    return false;
  }

  getFieldValue(field, data) {
    return (field === 'message' && typeof data.interactive_id === 'string' && data.interactive_id)
      ? data.interactive_id
      : this.getNestedValue(data, field);
  }

  evaluateSingleValue(field, operator, value, fieldValue) {
    const strField = String(fieldValue ?? '').toLowerCase();
    const strFieldExact = String(fieldValue ?? '');
    const valueArray = Array.isArray(value) ? value : [value];

    const cleanSystemPrefixes = (val) => {
      if (typeof val !== 'string') return val;
      let current = val;
      let previous;
      do {
        previous = current;
        current = current.replace(/^(f[0-9a-f]{6}|new[a-z0-9]+)___/i, '');
      } while (current !== previous);
      return current;
    };

    const getSuffix = (valStr) => {
      if (typeof valStr === 'string') {
        return cleanSystemPrefixes(valStr).toLowerCase();
      }
      return '';
    };

    const getSuffixExact = (valStr) => {
      if (typeof valStr === 'string') {
        return cleanSystemPrefixes(valStr);
      }
      return '';
    };

    switch (operator) {
      case 'equals':
      case 'equals_any': {
        return valueArray.some(v => {
          const strValueExact = String(v ?? '');
          const strValueLower = strValueExact.toLowerCase();
          if (strFieldExact.includes('___') || strValueExact.includes('___')) {
            return getSuffixExact(strFieldExact).toLowerCase() === getSuffixExact(strValueExact).toLowerCase();
          }
          return strField === strValueLower;
        });
      }
      case 'not_equals': {
        return valueArray.every(v => {
          const strValueExact = String(v ?? '');
          const strValueLower = strValueExact.toLowerCase();
          if (strFieldExact.includes('___') || strValueExact.includes('___')) {
            return getSuffixExact(strFieldExact).toLowerCase() !== getSuffixExact(strValueExact).toLowerCase();
          }
          return strField !== strValueLower;
        });
      }
      case 'contains':
      case 'contains_any': {
        return valueArray.some(v => {
          const strValue = String(v ?? '').toLowerCase();
          if (strField.includes('___') || strValue.includes('___')) {
            return getSuffix(strField).includes(getSuffix(strValue));
          }
          return strField.includes(strValue);
        });
      }
      case 'not_contains': {
        return valueArray.every(v => {
          const strValue = String(v ?? '').toLowerCase();
          if (strField.includes('___') || strValue.includes('___')) {
            return !getSuffix(strField).includes(getSuffix(strValue));
          }
          return !strField.includes(strValue);
        });
      }
      case 'starts_with':
        return valueArray.some(v => strField.startsWith(String(v ?? '').toLowerCase()));
      case 'ends_with':
        return valueArray.some(v => strField.endsWith(String(v ?? '').toLowerCase()));
      case 'greater_than':
        return Number(fieldValue) > Number(valueArray[0]);
      case 'less_than':
        return Number(fieldValue) < Number(valueArray[0]);
      case 'greater_than_or_equal':
        return Number(fieldValue) >= Number(valueArray[0]);
      case 'less_than_or_equal':
        return Number(fieldValue) <= Number(valueArray[0]);
      case 'is_empty':
        return !fieldValue || fieldValue === '';
      case 'is_not_empty':
      case 'exists':
        return !!fieldValue && fieldValue !== '';
      default:
        return false;
    }
  }
  getNestedValue(obj, path) {
    if (!obj || !path) return undefined;

    const directValue = path.split('.').reduce((current, key) => current?.[key], obj);
    if (directValue !== undefined) {
      return directValue;
    }

    if (path === 'contact_name' || path === 'sender.name') {
      return obj.contact?.name || obj.senderNumber || 'Customer';
    }
    if (path === 'phone_number' || path === 'sender_number' || path === 'sender.phone') {
      return obj.senderNumber || obj.contact?.phone_number;
    }

    if (typeof obj.message === 'string' && path.startsWith('message.')) {
      const messageSubPath = path.slice('message.'.length);
      if (
        messageSubPath === 'messageContext.text.body' ||
        messageSubPath === 'text.body' ||
        messageSubPath === 'body'
      ) {
        return obj.textContent || obj.message;
      }
    }

    if (obj.messagePayload) {
      const payloadValue = path.split('.').reduce((current, key) => current?.[key], { ...obj, message: obj.messagePayload });
      if (payloadValue !== undefined) {
        return payloadValue;
      }
    }

    if (obj.contact && obj.contact.custom_fields) {
      if (obj.contact.custom_fields[path] !== undefined) {
        return obj.contact.custom_fields[path];
      }

      const customValue = path.split('.').reduce((current, key) => current?.[key], obj.contact.custom_fields);
      if (customValue !== undefined) {
        return customValue;
      }
    }

    // Final fallback: if the field wasn't found anywhere and this object has a user reply
    // (set by auto-wait resume), return that. Handles arbitrary field names in conditions
    // after a send_message → condition flow pattern.
    if (obj.__auto_wait_reply !== undefined) {
      return obj.__auto_wait_reply;
    }

    return undefined;
  }


  normalizeMessagePayload(message) {
    if (message && typeof message === 'object') {
      return message;
    }

    const textBody = message == null ? '' : String(message);
    return {
      messageContext: {
        text: {
          body: textBody
        }
      }
    };
  }


  async executeActionNode(node, inputData) {
    const { action_type, parameters } = node.parameters || {};

    switch (action_type) {
      case 'log':
        console.log('Automation log:', parameters?.message || 'Action executed', inputData);
        break;
      case 'set_variable':
        const { variable_name, variable_value } = parameters || {};
        if (variable_name) {
          inputData[variable_name] = variable_value;
        }
        break;
      default:
        break;
    }

    return { success: true, output: inputData };
  }


  async executeRunSubFlowNode(node, flow, inputData, executionLog) {
    const { sub_flow_id } = node.parameters || {};

    if (!sub_flow_id) {
      return { success: false, output: inputData, error: 'sub_flow_id is required' };
    }

    if (inputData.flow_stack?.includes(sub_flow_id.toString())) {
      return { success: false, output: inputData, error: 'Circular sub-flow detected' };
    }

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'run_sub_flow', sub_flow_id });

      const { AutomationFlow } = await import('../models/index.js');
      const subFlow = await AutomationFlow.findById(sub_flow_id);

      if (subFlow) {
        const subResult = await this.processWorkflow(
          subFlow,
          null,
          {
            ...inputData,
            is_sub_flow: true,
            flow_stack: [...(inputData.flow_stack || []), flow._id.toString()]
          }
        );
        if (subResult && subResult.executionLog) {
          executionLog.push(...subResult.executionLog);
        }
        return { success: true, output: subResult.output || inputData };
      }
      return { success: true, output: inputData };
    }

    try {
      const { AutomationFlow } = await import('../models/index.js');
      const subFlow = await AutomationFlow.findOne({ _id: sub_flow_id, is_active: true, is_paused: { $ne: true }, deleted_at: null }).populate('user_id');

      if (!subFlow) {
        console.log(`[Run Sub Flow] Sub-flow ${sub_flow_id} not found, inactive, or paused. Skipping.`);
        return { success: false, output: inputData, error: 'Sub-flow not found, inactive, or paused' };
      }

      this.executeFlow(subFlow, {
        ...inputData,
        is_sub_flow: true,
        flow_stack: [...(inputData.flow_stack || []), flow._id.toString()]
      }).catch(err => console.error('[Run Sub Flow] Execution error:', err));

      return { success: true, output: { ...inputData, sub_flow_triggered: sub_flow_id } };
    } catch (error) {
      return { success: false, output: inputData, error: error.message };
    }
  }


  async executeDelayNode(node, inputData) {
    const { delay_ms } = node.parameters || {};
    let delay = (delay_ms !== undefined && delay_ms !== null && delay_ms !== '') ? parseInt(delay_ms) : 1000;
    if (isNaN(delay)) delay = 1000;

    if (inputData.is_test) {
      if (!inputData.test_responses) inputData.test_responses = [];
      inputData.test_responses.push({
        type: 'delay',
        delay_ms: delay
      });
      return { success: true, output: inputData };
    }

    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({ success: true, output: inputData });
      }, delay);
    });
  }


  async executeFilterNode(node, inputData) {
    const { filter_condition } = node.parameters || {};

    if (!filter_condition) {
      return { success: true, output: inputData };
    }

    const shouldPass = this.evaluateCondition(filter_condition, inputData);
    return { success: shouldPass, output: shouldPass ? inputData : {} };
  }


  async executeTransformNode(node, inputData) {
    const { transform_type, mapping } = node.parameters || {};

    let output = { ...inputData };

    if (transform_type === 'field_mapping' && mapping) {
      for (const [targetField, sourceField] of Object.entries(mapping)) {
        output[targetField] = this.getNestedValue(inputData, sourceField);
      }
    }

    return { success: true, output };
  }


  async executeWebhookNode(node, inputData) {
    const { url, method, headers, body } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'webhook', url, method });
      return { success: true, output: inputData };
    }

    if (!url) {
      return { success: false, output: inputData, error: 'Webhook URL is required' };
    }

    try {

      const processedBody = this.processTemplateString(JSON.stringify(body || {}), inputData);
      const processedUrl = this.processTemplateString(url, inputData);
      const processedHeaders = this.processHeaders(headers || {}, inputData);

      const response = await fetch(processedUrl, {
        method: method || 'POST',
        headers: processedHeaders,
        body: processedBody
      });

      const responseText = await response.text();
      const responseJson = this.isJsonString(responseText) ? JSON.parse(responseText) : responseText;

      return {
        success: response.ok,
        output: { ...inputData, webhook_response: responseJson, webhook_status: response.status }
      };
    } catch (error) {
      return { success: false, output: inputData, error: error.message };
    }
  }


  processTemplateString(template, data) {
    if (typeof template !== 'string') {
      return template;
    }


    let result = template.replace(/\{\{\{([^{}]+)\}\}\}/g, (match, path) => {
      const value = this.getNestedValue(data, path.trim());
      return value !== undefined ? value : match;
    });

    result = result.replace(/\{\{([^{}]+)\}\}/g, (match, path) => {
      const value = this.getNestedValue(data, path.trim());
      return value !== undefined ? value : match;
    });

    return result;
  }


  processHeaders(headers, data) {
    const processedHeaders = {};
    for (const [key, value] of Object.entries(headers)) {
      processedHeaders[key] = this.processTemplateString(value, data);
    }
    return processedHeaders;
  }


  isJsonString(str) {
    try {
      JSON.parse(str);
      return true;
    } catch (e) {
      return false;
    }
  }


  async executeFormFlowNode(node, inputData) {
    const {
      form_id,
      message_body,
      button_text,
      recipient,
      provider_type
    } = node.parameters || {};

    if (!form_id) {
      return { success: false, output: inputData, error: 'Form ID is required' };
    }

    try {
      const userId = inputData.userId || inputData.user_id;
      if (!userId) {
        return { success: false, output: inputData, error: 'User ID is required' };
      }

      const rawRecipient = recipient || inputData.senderNumber || inputData.phone_number;
      if (!rawRecipient) {
        return { success: false, output: inputData, error: 'Recipient is required' };
      }

      const processedRecipient = this.processTemplateString(rawRecipient, inputData);
      const processedBody = message_body ? this.processTemplateString(message_body, inputData) : 'Please fill out this form';
      const processedButton = button_text ? this.processTemplateString(button_text, inputData) : 'Open Form';

      const { Form } = await import('../models/index.js');
      const form = await Form.findOne({ _id: form_id, user_id: userId, deleted_at: null });

      if (!form || !form.flow?.flow_id) {
        return { success: false, output: inputData, error: 'Form not found or has no published Meta Flow' };
      }

      const messageParams = {
        recipientNumber: processedRecipient,
        messageType: 'interactive',
        interactiveType: 'flow',
        messageText: processedBody,
        flowId: form.flow.flow_id,
        flowToken: `flow_${Date.now()}`,
        buttonText: processedButton,
        providerType: provider_type || PROVIDER_TYPES.BUSINESS_API
      };

      if (inputData.is_test) {
        messageParams.buttonParams = [{ title: processedButton, id: `form_${form_id}` }];
        inputData.test_responses = inputData.test_responses || [];
        inputData.test_responses.push({
          node_id: node.id,
          type: 'form_flow',
          message: processedBody,
          button: processedButton,
          form_id: form_id,
          messageParams: messageParams
        });
        return {
          success: true,
          output: {
            ...inputData,
            form_sent: true,
            sent_to: processedRecipient,
            message_id: `sim_form_${Date.now()}`
          }
        };
      }

      let contactDoc = null;
      const contactId = inputData.contactId || inputData.contact?._id;
      if (contactId) {
        contactDoc = await Contact.findOne({ _id: contactId, created_by: userId, deleted_at: null }).lean();
      } else {
        contactDoc = await Contact.findOne({
          $or: [
            { phone_number: processedRecipient },
            { telegram_chat_id: processedRecipient }
          ],
          created_by: userId,
          deleted_at: null
        }).lean();
      }

      if (contactDoc && contactDoc.source === 'telegram') {
        const { default: TelegramConnection } = await import('../models/telegram-connection.model.js');
        const bot = await TelegramConnection.findOne({ user_id: userId, is_active: true });
        if (!bot) {
          throw new Error('No active Telegram bot found for user');
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');
        const result = await omnichannelService.sendMessage({
          platform: 'telegram',
          workspace_id: bot.workspace_id,
          user_id: userId,
          recipient_id: contactDoc.telegram_chat_id,
          message_type: 'text',
          text: `${processedBody}\n\n(Note: Forms/Flows are only supported on WhatsApp)`
        });

        const telegramMsgId = result?.message_id?.toString() || `tg_${Date.now()}`;

        const newMessage = await Message.create({
          workspace_id: bot.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: 'telegram',
          provider: 'telegram',
          sender_id: bot.bot_id,
          recipient_id: contactDoc.telegram_chat_id,
          direction: 'outbound',
          message_type: 'text',
          content: `${processedBody} (WhatsApp Flow)`,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: bot.bot_id,
              name: bot.bot_id
            },
            recipient: {
              id: contactDoc.telegram_chat_id,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: 'telegram',
            provider: 'telegram'
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return {
          success: true,
          output: {
            ...inputData,
            form_sent: true,
            sent_to: contactDoc.telegram_chat_id,
            message_id: telegramMsgId
          }
        };
      } else if (contactDoc && (contactDoc.source === 'facebook' || contactDoc.source === 'instagram')) {
        const platform = contactDoc.source;
        let connection = null;
        if (platform === 'facebook') {
          const { default: FacebookConnection } = await import('../models/facebook-connection.model.js');
          connection = await FacebookConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await FacebookConnection.findOne({ user_id: userId, is_active: true });
          }
        } else {
          const { default: InstagramConnection } = await import('../models/instagram-connection.model.js');
          connection = await InstagramConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await InstagramConnection.findOne({ user_id: userId, is_active: true });
          }
        }

        if (!connection) {
          throw new Error(`No active ${platform} connection found for user`);
        }

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
          if (!pageId) throw new Error(`No active Facebook Page found for this connection`);
        } else {
          if (!pageId) {
            pageId = connection.ig_user_id || (connection.pages && connection.pages[0]?.instagram_account_id);
          }
          if (!pageId) throw new Error(`No active Instagram Account found for this connection`);
        }

        const recipientId = platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id;
        if (!recipientId) {
          throw new Error(`Recipient scoped ID not found for contact`);
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');
        const result = await omnichannelService.sendMessage({
          platform,
          workspace_id: connection.workspace_id,
          user_id: userId,
          page_id: pageId,
          recipient_id: recipientId,
          message_type: 'text',
          text: `${processedBody}\n\n(Note: Interactive Forms are only supported on WhatsApp)`
        });

        const fbIgMsgId = result?.message_id?.toString() || `fbig_${Date.now()}`;

        const newMessage = await Message.create({
          workspace_id: connection.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: platform,
          provider: platform,
          sender_id: pageId,
          recipient_id: recipientId,
          direction: 'outbound',
          message_type: 'text',
          content: `${processedBody} (WhatsApp Flow)`,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: pageId,
              name: pageId
            },
            recipient: {
              id: recipientId,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: platform,
            provider: platform
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return {
          success: true,
          output: {
            ...inputData,
            form_sent: true,
            sent_to: recipientId,
            message_id: fbIgMsgId
          }
        };
      }

      if (inputData.whatsappPhoneNumberId) {
        const whatsappPhoneNumber = await WhatsappPhoneNumber.findById(inputData.whatsappPhoneNumberId)
          .populate('waba_id')
          .lean();

        if (whatsappPhoneNumber && whatsappPhoneNumber.waba_id) {
          messageParams.whatsappPhoneNumber = whatsappPhoneNumber;
        }
      }

      const result = await unifiedWhatsAppService.sendMessage(userId, messageParams);

      return {
        success: true,
        output: {
          ...inputData,
          form_sent: true,
          sent_to: processedRecipient,
          message_id: result.messageId
        }
      };
    } catch (error) {
      return { success: false, output: inputData, error: error.message };
    }
  }


  async executeAIResponseNode(node, inputData) {
    const { ai_model, prompt_template, api_key } = node.parameters || {};

    if (!ai_model || !prompt_template) {
      return { success: false, output: inputData, error: 'AI model and prompt are required' };
    }

    try {
      const processedPrompt = this.processTemplateString(prompt_template, inputData);

      const aiResponse = `AI response for: ${processedPrompt.substring(0, 50)}...`;

      return {
        success: true,
        output: { ...inputData, ai_response: aiResponse }
      };
    } catch (error) {
      return { success: false, output: inputData, error: error.message };
    }
  }


  async executePollNode(node, inputData) {
    // Forward the poll node to the unified send message handler which has poll parameter processing
    return await this.executeSendMessageNode(node, inputData);
  }

  async executeSendMessageNode(node, inputData) {
    const {
      recipient,
      message_body,
      message_template,
      media_url,
      mediaUrl,
      buttons,
      interactive_type,
      button_params,
      list_params,
      provider_type,
      messageType,
      location_params
    } = node.parameters || {};

    const activeMediaUrl = media_url || mediaUrl;

    if (!recipient) {
      console.error('[Send Message Node] Error: Recipient is missing');
      return { success: false, output: inputData, error: 'Recipient is required' };
    }

    try {
      const userId = inputData.userId || inputData.user_id;
      console.log(`[Send Message Node] Sending to ${recipient} (User: ${userId})`);
      if (!userId) {
        console.error('No userId found in inputData:', inputData);
        return { success: false, output: inputData, error: 'User ID is required to send message' };
      }

      const processedRecipient = this.processTemplateString(recipient, inputData);

      const messageParams = {
        recipientNumber: processedRecipient,
        replyType: node.parameters?.reply_material_id ? 'flow' : undefined,
        replyId: node.parameters?.reply_material_id,
        providerType: provider_type || PROVIDER_TYPES.BUSINESS_API
      };

      const flowIdStr = inputData.flow_id?.toString() || '';
      const flowPrefix = flowIdStr ? `f${flowIdStr.slice(-6)}` : '';

      if (messageType === 'location' && location_params) {
        messageParams.messageType = 'location';
        messageParams.locationParams = {
          latitude: location_params.latitude,
          longitude: location_params.longitude,
          name: location_params.name || this.processTemplateString(location_params.name || '', inputData),
          address: location_params.address || this.processTemplateString(location_params.address || '', inputData)
        };
      } else {
        if (message_body || message_template || node.parameters?.message || node.parameters?.bodyText) {
          const rawMessage = message_body || message_template || node.parameters?.message || node.parameters?.bodyText;
          const processedMessage = this.processTemplateString(rawMessage, inputData);
          messageParams.messageText = processedMessage;
        }
      }
      if (activeMediaUrl) {
        const resolvedUrl = businessApiProvider.getPublicMediaUrl(activeMediaUrl);

        messageParams.mediaUrl = resolvedUrl;
        messageParams.file = {
          originalname: 'media',
          mimetype: this.getMimeTypeFromUrl(resolvedUrl),
          buffer: null,
          url: resolvedUrl
        };
      }

      if (interactive_type) {
        messageParams.messageType = 'interactive';
        messageParams.interactiveType = interactive_type;

        if (interactive_type === 'button' && button_params) {
          messageParams.buttonParams = button_params.map((btn, index) => {
            const rawId = btn.id || `btn_${index + 1}`;
            const id = (flowPrefix && !rawId.startsWith(`${flowPrefix}___`)) ? `${flowPrefix}___${rawId}` : rawId;
            return {
              title: this.processTemplateString(btn.title, inputData),
              id: id
            };
          });
        } else if (interactive_type === 'list' && list_params) {
          messageParams.listParams = {
            header: this.processTemplateString(list_params.header || '', inputData),
            body: this.processTemplateString(list_params.body || message_template || node.parameters?.message || node.parameters?.bodyText || '', inputData),
            footer: this.processTemplateString(list_params.footer || '', inputData),
            buttonTitle: this.processTemplateString(list_params.buttonTitle || 'Select', inputData),
            sectionTitle: this.processTemplateString(list_params.sectionTitle || 'Options', inputData),
            items: (list_params.items || []).map((item, index) => {
              const rawId = item.id || item.title || `item_${index + 1}`;
              const id = (flowPrefix && !rawId.startsWith(`${flowPrefix}___`)) ? `${flowPrefix}___${rawId}` : rawId;
              return {
                title: this.processTemplateString(item.title, inputData),
                description: this.processTemplateString(item.description || '', inputData),
                id: id
              };
            })
          };
        }
      } else if (buttons && Array.isArray(buttons) && buttons.length > 0 && buttons.length <= 3) {
        messageParams.buttons = buttons;
        messageParams.messageType = 'interactive';
        messageParams.interactiveType = 'button';
        messageParams.buttonParams = buttons.map((btn, index) => {
          const rawId = btn.value || btn.id || `btn_${index + 1}`;
          const id = (flowPrefix && !rawId.startsWith(`${flowPrefix}___`)) ? `${flowPrefix}___${rawId}` : rawId;
          return {
            id: id,
            title: this.processTemplateString(btn.text || btn.title || '', inputData)
          };
        });
      } else {
        if (messageParams.file) {
          const mime = messageParams.file.mimetype;
          if (mime.startsWith('image')) messageParams.messageType = 'image';
          else if (mime.startsWith('video')) messageParams.messageType = 'video';
          else if (mime.startsWith('audio')) messageParams.messageType = 'audio';
          else messageParams.messageType = 'document';
        } else if (!messageParams.messageType) {
          messageParams.messageType = 'text';
        }
      }

      if (inputData.is_test) {
        inputData.test_responses = inputData.test_responses || [];
        inputData.test_responses.push({
          node_id: node.id,
          type: 'send_message',
          messageParams: messageParams
        });
        return {
          success: true,
          output: {
            ...inputData,
            message_sent: true,
            sent_to: processedRecipient,
            provider: 'simulation',
            message_id: `sim_msg_${Date.now()}`
          }
        };
      }

      let contactDoc = null;
      const contactId = inputData.contactId || inputData.contact?._id;
      if (contactId) {
        contactDoc = await Contact.findOne({ _id: contactId, created_by: userId, deleted_at: null }).lean();
      } else {
        contactDoc = await Contact.findOne({
          $or: [
            { phone_number: processedRecipient },
            { telegram_chat_id: processedRecipient },
            { facebook_page_scoped_id: processedRecipient },
            { instagram_scoped_id: processedRecipient }
          ],
          created_by: userId,
          deleted_at: null
        }).lean();
      }

      if (contactDoc && contactDoc.source === 'telegram') {
        const { default: TelegramConnection } = await import('../models/telegram-connection.model.js');
        const bot = await TelegramConnection.findOne({ user_id: userId, is_active: true });
        if (!bot) {
          throw new Error('No active Telegram bot found for user');
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');

        let msgType = messageParams.messageType || 'text';
        let textToSend = messageParams.messageText;

        let buttonsToPass = messageParams.buttonParams || messageParams.buttons;

        if (msgType === 'interactive') {
          if (messageParams.interactiveType === 'list' && messageParams.listParams) {
            const listItems = messageParams.listParams.items || [];
            buttonsToPass = listItems.map(item => ({
              id: item.id || item.title,
              text: item.title
            }));
            if (!textToSend) {
              textToSend = messageParams.listParams.body || messageParams.listParams.header || "Please select an option:";
            }
          }
        }

        const result = await omnichannelService.sendMessage({
          platform: 'telegram',
          workspace_id: bot.workspace_id,
          user_id: userId,
          recipient_id: contactDoc.telegram_chat_id,
          message_type: msgType,
          text: textToSend,
          file_url: messageParams.mediaUrl,
          buttons: buttonsToPass,
          latitude: messageParams.locationParams?.latitude,
          longitude: messageParams.locationParams?.longitude,
          name: messageParams.locationParams?.name,
          address: messageParams.locationParams?.address
        });

        const telegramMsgId = result?.message_id?.toString() || `tg_${Date.now()}`;

        const tgInteractiveData = msgType === 'interactive' ? {
          interactiveType: messageParams.interactiveType,
          buttons: messageParams.interactiveType === 'button' ? messageParams.buttonParams : undefined,
          list: messageParams.interactiveType === 'list' ? messageParams.listParams : undefined
        } : null;

        let tgFileUrl = messageParams.mediaUrl || null;

        const newMessage = await Message.create({
          workspace_id: bot.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: 'telegram',
          provider: 'telegram',
          sender_id: bot.bot_id,
          recipient_id: contactDoc.telegram_chat_id,
          direction: 'outbound',
          message_type: msgType,
          content: msgType === 'location'
            ? (messageParams.locationParams?.name || messageParams.locationParams?.address || '📍 Location')
            : (textToSend || messageParams.mediaUrl || 'Media message'),
          file_url: tgFileUrl,
          interactive_data: tgInteractiveData,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: bot.bot_id,
              name: bot.bot_id
            },
            recipient: {
              id: contactDoc.telegram_chat_id,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: 'telegram',
            provider: 'telegram'
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return {
          success: true,
          output: {
            ...inputData,
            message_sent: true,
            sent_to: contactDoc.telegram_chat_id,
            provider: 'telegram',
            message_id: telegramMsgId
          }
        };
      } else if (contactDoc && (contactDoc.source === 'facebook' || contactDoc.source === 'instagram')) {
        const platform = contactDoc.source;
        let connection = null;
        if (platform === 'facebook') {
          const { default: FacebookConnection } = await import('../models/facebook-connection.model.js');
          connection = await FacebookConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await FacebookConnection.findOne({ user_id: userId, is_active: true });
          }
        } else {
          const { default: InstagramConnection } = await import('../models/instagram-connection.model.js');
          connection = await InstagramConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await InstagramConnection.findOne({ user_id: userId, is_active: true });
          }
        }

        if (!connection) {
          throw new Error(`No active ${platform} connection found for user`);
        }

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
          if (!pageId) throw new Error(`No active Facebook Page found for this connection`);
        } else {
          if (!pageId) {
            pageId = connection.ig_user_id || (connection.pages && connection.pages[0]?.instagram_account_id);
          }
          if (!pageId) throw new Error(`No active Instagram Account found for this connection`);
        }

        const recipientId = platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id;
        if (!recipientId) {
          throw new Error(`Recipient scoped ID not found for contact`);
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');

        let msgType = messageParams.messageType || 'text';
        let textToSend = messageParams.messageText;

        let buttonsToPass = messageParams.buttonParams || messageParams.buttons;

        if (msgType === 'interactive') {
          if (messageParams.interactiveType === 'list' && messageParams.listParams) {
            const listItems = messageParams.listParams.items || [];
            buttonsToPass = listItems.map(item => ({
              id: item.id || item.title,
              text: item.title
            }));
            if (!textToSend) {
              textToSend = messageParams.listParams.body || messageParams.listParams.header || "Please select an option:";
            }
          }
        }

        const result = await omnichannelService.sendMessage({
          platform,
          workspace_id: connection.workspace_id,
          user_id: userId,
          page_id: pageId,
          recipient_id: recipientId,
          message_type: msgType,
          text: textToSend,
          file_url: messageParams.mediaUrl,
          buttons: buttonsToPass,
          latitude: messageParams.locationParams?.latitude,
          longitude: messageParams.locationParams?.longitude,
          name: messageParams.locationParams?.name,
          address: messageParams.locationParams?.address
        });

        const fbIgMsgId = result?.message_id?.toString() || `fbig_${Date.now()}`;

        const fbIgInteractiveData = msgType === 'interactive' ? {
          interactiveType: messageParams.interactiveType,
          buttons: messageParams.interactiveType === 'button' ? messageParams.buttonParams : undefined,
          list: messageParams.interactiveType === 'list' ? messageParams.listParams : undefined
        } : null;

        let fbIgFileUrl = messageParams.mediaUrl || null;


        const newMessage = await Message.create({
          workspace_id: connection.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: platform,
          provider: platform,
          sender_id: pageId,
          recipient_id: recipientId,
          direction: 'outbound',
          message_type: msgType,
          content: msgType === 'location'
            ? (messageParams.locationParams?.name || messageParams.locationParams?.address || '📍 Location')
            : (textToSend || messageParams.mediaUrl || 'Media message'),
          file_url: fbIgFileUrl,
          interactive_data: fbIgInteractiveData,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: pageId,
              name: pageId
            },
            recipient: {
              id: recipientId,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: platform,
            provider: platform
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return {
          success: true,
          output: {
            ...inputData,
            message_sent: true,
            sent_to: recipientId,
            provider: platform,
            message_id: fbIgMsgId
          }
        };
      }

      if (inputData.whatsappPhoneNumberId) {
        const whatsappPhoneNumber = await WhatsappPhoneNumber.findById(inputData.whatsappPhoneNumberId)
          .populate('waba_id')
          .lean();

        if (whatsappPhoneNumber && whatsappPhoneNumber.waba_id) {
          messageParams.whatsappPhoneNumber = whatsappPhoneNumber;
        }
      } else if (inputData.whatsappConnectionId) {
        messageParams.connectionId = inputData.whatsappConnectionId;
      }

      const result = await unifiedWhatsAppService.sendMessage(userId, messageParams);

      return {
        success: true,
        output: {
          ...inputData,
          message_sent: true,
          sent_to: processedRecipient,
          provider: result.provider,
          message_id: result.messageId
        }
      };
    } catch (error) {
      return { success: false, output: inputData, error: error.message };
    }
  }


  async executeSendTemplateNode(node, inputData) {
    const {
      template_id,
      template_name,
      recipient,
      language_code,
      body_variables,
      header_media_url,
      carousel_cards_data,
      carousel_products,
      coupon_code,
      offer_expiration_minutes,
      product_retailer_id,
      url_button_value,
      provider_type
    } = node.parameters || {};

    if (!recipient) {
      return { success: false, output: inputData, error: 'Recipient is required for send_template node' };
    }

    const processedRecipient = this.processTemplateString(recipient, inputData);

    if (!template_id && !template_name) {
      return { success: false, output: inputData, error: 'template_id or template_name is required' };
    }

    const userId = inputData.userId || inputData.user_id;
    if (!userId) {
      return { success: false, output: inputData, error: 'userId is required to send template' };
    }

    try {
      let templateQuery;
      if (template_id) {
        templateQuery = {
          _id: template_id,
          $or: [{ user_id: userId }, { is_admin_template: true }, { user_id: null }, { user_id: { $exists: false } }]
        };
      } else {
        templateQuery = {
          template_name: template_name.toLowerCase(),
          $or: [{ user_id: userId }, { is_admin_template: true }, { user_id: null }, { user_id: { $exists: false } }]
        };
      }

      console.log(`[send_template] Looking up template:`, JSON.stringify(templateQuery));
      const template = await Template.findOne(templateQuery).lean();

      if (!template) {
        console.error(`[send_template] Template NOT found. Query:`, JSON.stringify(templateQuery));
        return {
          success: false,
          output: inputData,
          error: `Template not found: ${template_id || template_name}`
        };
      }

      console.log(`[send_template] Found template: "${template.template_name}" (type: ${template.template_type}, status: ${template.status})`);


      if (template.status !== 'approved') {
        return {
          success: false,
          output: inputData,
          error: `Template "${template.template_name}" is not approved (status: ${template.status})`
        };
      }

      const resolvedBodyVars = {};
      if (body_variables && typeof body_variables === 'object') {
        for (const [key, val] of Object.entries(body_variables)) {
          resolvedBodyVars[key] = this.processTemplateString(String(val ?? ''), inputData);
        }
      }

      const resolvedHeaderMediaUrl = header_media_url
        ? this.processTemplateString(header_media_url, inputData)
        : null;

      let resolvedCarouselCardsData = null;
      if (Array.isArray(carousel_cards_data) && carousel_cards_data.length > 0) {
        resolvedCarouselCardsData = carousel_cards_data.map(card => ({
          ...card,
          header: card.header
            ? {
              ...card.header,
              link: card.header.link
                ? this.processTemplateString(card.header.link, inputData)
                : undefined
            }
            : undefined,
          buttons: Array.isArray(card.buttons)
            ? card.buttons.map(btn => ({
              ...btn,
              url_value: btn.url_value
                ? this.processTemplateString(btn.url_value, inputData)
                : undefined,
              payload: btn.payload
                ? this.processTemplateString(btn.payload, inputData)
                : undefined
            }))
            : []
        }));
      }

      let resolvedCarouselProducts = null;
      if (Array.isArray(carousel_products) && carousel_products.length > 0) {
        resolvedCarouselProducts = carousel_products.map(p => ({
          product_retailer_id: this.processTemplateString(p.product_retailer_id, inputData),
          catalog_id: this.processTemplateString(p.catalog_id, inputData)
        }));
      }

      const messageParams = {
        recipientNumber: processedRecipient,
        messageType: 'template',
        templateName: template.template_name,
        languageCode: language_code || template.language || 'en_US',
        templateObj: template,
        templateVariables: resolvedBodyVars,
        providerType: provider_type || PROVIDER_TYPES.BUSINESS_API
      };

      if (resolvedHeaderMediaUrl) {
        messageParams.mediaUrl = resolvedHeaderMediaUrl;
      }

      if (resolvedCarouselCardsData) {
        messageParams.carouselCardsData = resolvedCarouselCardsData;
      }

      if (resolvedCarouselProducts) {
        messageParams.carouselProducts = resolvedCarouselProducts;
      }

      if (coupon_code) {
        messageParams.couponCode = this.processTemplateString(coupon_code, inputData);
      }

      if (offer_expiration_minutes !== undefined) {
        messageParams.offerExpirationMinutes = Number(offer_expiration_minutes);
      }

      if (product_retailer_id) {
        messageParams.productRetailerId = this.processTemplateString(product_retailer_id, inputData);
      }

      if (url_button_value) {
        if (!messageParams.templateVariables) messageParams.templateVariables = {};
        messageParams.templateVariables.url = this.processTemplateString(url_button_value, inputData);
      }

      if (inputData.is_test) {
        inputData.test_responses = inputData.test_responses || [];

        let bodyText = template.message_body || '';
        for (const [key, val] of Object.entries(resolvedBodyVars)) {
          bodyText = bodyText.replace(new RegExp(`\\{\\{${key}\\}\\}`, 'g'), val);
        }

        let simText = '';
        if (template.header && template.header.format === 'text' && template.header.text) {
          simText += '*' + template.header.text + '*\n\n';
        }
        simText += bodyText;
        if (template.footer_text) {
          simText += '\n\n_' + template.footer_text + '_';
        }

        const resolvedCoupon = coupon_code ? this.processTemplateString(coupon_code, inputData) : null;
        if (resolvedCoupon) {
          simText += `\n\nCoupon Code: ${resolvedCoupon}`;
        }

        messageParams.messageText = simText;

        if (!messageParams.mediaUrl && template.header && template.header.format === 'media' && template.header.media_url) {
          messageParams.mediaUrl = template.header.media_url;
        }

        if (Array.isArray(template.buttons)) {
          messageParams.buttonParams = template.buttons.map((btn, index) => {
            if (btn.type === 'url' || btn.type === 'website') {
              let btnUrl = btn.url || btn.website_url || '';
              if (url_button_value) {
                btnUrl = this.processTemplateString(url_button_value, inputData);
              }
              return { title: btn.text, id: btnUrl };
            }
            return {
              title: btn.text,
              id: btn.value || btn.id || `btn_${index + 1}`
            };
          });
        }

        inputData.test_responses.push({
          node_id: node.id,
          type: 'send_template',
          templateName: template.template_name,
          messageParams: messageParams
        });
        return {
          success: true,
          output: {
            ...inputData,
            template_sent: true,
            template_name: template.template_name,
            template_type: template.template_type,
            sent_to: processedRecipient,
            provider: 'simulation',
            message_id: `sim_tpl_${Date.now()}`
          }
        };
      }

      let contactDoc = null;
      const contactId = inputData.contactId || inputData.contact?._id;
      if (contactId) {
        contactDoc = await Contact.findOne({ _id: contactId, created_by: userId, deleted_at: null }).lean();
      } else {
        contactDoc = await Contact.findOne({
          $or: [
            { phone_number: processedRecipient },
            { telegram_chat_id: processedRecipient },
            { facebook_page_scoped_id: processedRecipient },
            { instagram_scoped_id: processedRecipient }
          ],
          created_by: userId,
          deleted_at: null
        }).lean();
      }

      if (contactDoc && contactDoc.source === 'telegram') {
        const { default: TelegramConnection } = await import('../models/telegram-connection.model.js');
        const bot = await TelegramConnection.findOne({ user_id: userId, is_active: true });
        if (!bot) {
          throw new Error('No active Telegram bot found for user');
        }

        let bodyText = template.message_body || '';
        for (const [key, val] of Object.entries(resolvedBodyVars)) {
          bodyText = bodyText.replace(new RegExp(`\\{\\{${key}\\}\\}`, 'g'), val);
        }

        let telegramText = '';
        if (template.header && template.header.format === 'text' && template.header.text) {
          telegramText += template.header.text + '\n\n';
        }
        telegramText += bodyText;
        if (template.footer_text) {
          telegramText += '\n\n' + template.footer_text;
        }

        const resolvedCoupon = coupon_code ? this.processTemplateString(coupon_code, inputData) : null;
        if (resolvedCoupon) {
          telegramText += `\n\nCoupon Code: ${resolvedCoupon}`;
        }

        let buttonsToPass = [];
        if (Array.isArray(template.buttons)) {
          buttonsToPass = template.buttons.map((btn, index) => {
            if (btn.type === 'url' || btn.type === 'website') {
              let btnUrl = btn.url || btn.website_url || '';
              if (url_button_value) {
                btnUrl = this.processTemplateString(url_button_value, inputData);
              }
              return {
                text: btn.text,
                url: btnUrl,
                type: 'url'
              };
            }
            return {
              text: btn.text,
              id: btn.value || btn.id || `btn_${index + 1}`
            };
          });
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');

        let msgType = 'text';
        let mediaUrlToSend = resolvedHeaderMediaUrl;
        if (!mediaUrlToSend && template.header && template.header.format === 'media' && template.header.media_url) {
          mediaUrlToSend = template.header.media_url;
        }

        if (mediaUrlToSend) {
          const extension = mediaUrlToSend.split('.').pop().toLowerCase().split('?')[0];
          if (['jpg', 'jpeg', 'png', 'webp', 'gif'].includes(extension)) msgType = 'image';
          else if (['mp4', 'mov', 'avi', 'mkv'].includes(extension)) msgType = 'video';
          else if (['mp3', 'ogg', 'wav', 'm4a'].includes(extension)) msgType = 'audio';
          else msgType = 'document';
        }

        const result = await omnichannelService.sendMessage({
          platform: 'telegram',
          workspace_id: bot.workspace_id,
          user_id: userId,
          recipient_id: contactDoc.telegram_chat_id,
          message_type: msgType,
          text: telegramText,
          file_url: mediaUrlToSend,
          buttons: buttonsToPass
        });

        const telegramMsgId = result?.message_id?.toString() || `tg_${Date.now()}`;

        const newMessage = await Message.create({
          workspace_id: bot.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: 'telegram',
          provider: 'telegram',
          sender_id: bot.bot_id,
          recipient_id: contactDoc.telegram_chat_id,
          direction: 'outbound',
          message_type: msgType,
          content: telegramText || mediaUrlToSend || 'Template message',
          file_url: mediaUrlToSend,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: bot.bot_id,
              name: bot.bot_id
            },
            recipient: {
              id: contactDoc.telegram_chat_id,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: 'telegram',
            provider: 'telegram'
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return {
          success: true,
          output: {
            ...inputData,
            template_sent: true,
            template_name: template.template_name,
            template_type: template.template_type,
            sent_to: contactDoc.telegram_chat_id,
            provider: 'telegram',
            message_id: telegramMsgId
          }
        };
      } else if (contactDoc && (contactDoc.source === 'facebook' || contactDoc.source === 'instagram')) {
        const platform = contactDoc.source;
        let connection = null;
        if (platform === 'facebook') {
          const { default: FacebookConnection } = await import('../models/facebook-connection.model.js');
          connection = await FacebookConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await FacebookConnection.findOne({ user_id: userId, is_active: true });
          }
        } else {
          const { default: InstagramConnection } = await import('../models/instagram-connection.model.js');
          connection = await InstagramConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await InstagramConnection.findOne({ user_id: userId, is_active: true });
          }
        }

        if (!connection) {
          throw new Error(`No active ${platform} connection found for user`);
        }

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
          if (!pageId) throw new Error(`No active Facebook Page found for this connection`);
        } else {
          if (!pageId) {
            pageId = connection.ig_user_id || (connection.pages && connection.pages[0]?.instagram_account_id);
          }
          if (!pageId) throw new Error(`No active Instagram Account found for this connection`);
        }

        const recipientId = platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id;
        if (!recipientId) {
          throw new Error(`Recipient scoped ID not found for contact`);
        }

        let bodyText = template.message_body || '';
        for (const [key, val] of Object.entries(resolvedBodyVars)) {
          bodyText = bodyText.replace(new RegExp(`\\{\\{${key}\\}\\}`, 'g'), val);
        }

        let fbIgText = '';
        if (template.header && template.header.format === 'text' && template.header.text) {
          fbIgText += template.header.text + '\n\n';
        }
        fbIgText += bodyText;
        if (template.footer_text) {
          fbIgText += '\n\n' + template.footer_text;
        }

        const resolvedCoupon = coupon_code ? this.processTemplateString(coupon_code, inputData) : null;
        if (resolvedCoupon) {
          fbIgText += `\n\nCoupon Code: ${resolvedCoupon}`;
        }

        let buttonsToPass = [];
        if (Array.isArray(template.buttons)) {
          buttonsToPass = template.buttons.map((btn, index) => {
            if (btn.type === 'url' || btn.type === 'website') {
              let btnUrl = btn.url || btn.website_url || '';
              if (url_button_value) {
                btnUrl = this.processTemplateString(url_button_value, inputData);
              }
              return {
                text: btn.text,
                url: btnUrl,
                type: 'url'
              };
            }
            return {
              text: btn.text,
              id: btn.value || btn.id || `btn_${index + 1}`
            };
          });
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');

        let msgType = 'text';
        let mediaUrlToSend = resolvedHeaderMediaUrl;
        if (!mediaUrlToSend && template.header && template.header.format === 'media' && template.header.media_url) {
          mediaUrlToSend = template.header.media_url;
        }

        if (mediaUrlToSend) {
          const extension = mediaUrlToSend.split('.').pop().toLowerCase().split('?')[0];
          if (['jpg', 'jpeg', 'png', 'webp', 'gif'].includes(extension)) msgType = 'image';
          else if (['mp4', 'mov', 'avi', 'mkv'].includes(extension)) msgType = 'video';
          else if (['mp3', 'ogg', 'wav', 'm4a'].includes(extension)) msgType = 'audio';
          else msgType = 'document';
        }

        const result = await omnichannelService.sendMessage({
          platform,
          workspace_id: connection.workspace_id,
          user_id: userId,
          page_id: pageId,
          recipient_id: recipientId,
          message_type: msgType,
          text: fbIgText,
          file_url: mediaUrlToSend,
          buttons: buttonsToPass
        });

        const fbIgMsgId = result?.message_id?.toString() || `fbig_${Date.now()}`;

        const newMessage = await Message.create({
          workspace_id: connection.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: platform,
          provider: platform,
          sender_id: pageId,
          recipient_id: recipientId,
          direction: 'outbound',
          message_type: msgType,
          content: fbIgText || mediaUrlToSend || 'Template message',
          file_url: mediaUrlToSend,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: pageId,
              name: pageId
            },
            recipient: {
              id: recipientId,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: platform,
            provider: platform
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return {
          success: true,
          output: {
            ...inputData,
            template_sent: true,
            template_name: template.template_name,
            template_type: template.template_type,
            sent_to: recipientId,
            provider: platform,
            message_id: fbIgMsgId
          }
        };
      }

      if (inputData.whatsappPhoneNumberId) {
        const whatsappPhoneNumber = await WhatsappPhoneNumber.findById(inputData.whatsappPhoneNumberId)
          .populate('waba_id')
          .lean();
        if (whatsappPhoneNumber && whatsappPhoneNumber.waba_id) {
          messageParams.whatsappPhoneNumber = whatsappPhoneNumber;
        }
      } else if (inputData.whatsappConnectionId) {
        messageParams.connectionId = inputData.whatsappConnectionId;
      }

      const result = await unifiedWhatsAppService.sendMessage(userId, messageParams);

      return {
        success: true,
        output: {
          ...inputData,
          template_sent: true,
          template_name: template.template_name,
          template_type: template.template_type,
          sent_to: processedRecipient,
          provider: result?.provider,
          message_id: result?.messageId
        }
      };
    } catch (error) {
      console.error('Error in executeSendTemplateNode:', error);
      return { success: false, output: inputData, error: error.message };
    }
  }


  getMimeTypeFromUrl(url) {
    const lowerUrl = url.toLowerCase();
    if (lowerUrl.includes('.jpg') || lowerUrl.includes('.jpeg')) return 'image/jpeg';
    if (lowerUrl.includes('.png')) return 'image/png';
    if (lowerUrl.includes('.mp4')) return 'video/mp4';
    if (lowerUrl.includes('.mp3')) return 'audio/mp3';
    if (lowerUrl.includes('.pdf')) return 'application/pdf';
    return 'application/octet-stream';
  }


  async executeUpdateContactNode(node, inputData) {
    const { updates } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'update_contact', updates });
      return { success: true, output: inputData };
    }

    const userId = inputData.userId || inputData.user_id;
    const contactId = inputData.contactId || inputData.contact?._id;

    if (!userId) {
      return { success: false, output: inputData, error: 'User ID is required to update contact' };
    }
    if (!contactId) {
      return { success: false, output: inputData, error: 'contactId is required to update contact' };
    }

    try {
      const { Contact } = await import('../models/index.js');
      const standardFields = ['name', 'email', 'phone_number', 'status', 'source'];
      const fieldUpdates = {};
      const customFieldUpdates = {};

      if (Array.isArray(updates)) {
        for (const update of updates) {
          let { field_key, value } = update;
          if (!field_key) continue;

          if (field_key === 'phone') {
            field_key = 'phone_number';
          }

          const resolvedValue = typeof value === 'string' ? this.processTemplateString(value, inputData) : value;

          if (standardFields.includes(field_key)) {
            fieldUpdates[field_key] = resolvedValue;
          } else {
            customFieldUpdates[`custom_fields.${field_key}`] = resolvedValue;
          }
        }
      }

      const finalUpdates = { ...fieldUpdates, ...customFieldUpdates };

      if (Object.keys(finalUpdates).length > 0) {
        await Contact.updateOne(
          { _id: contactId, created_by: userId, deleted_at: null },
          { $set: finalUpdates }
        );
      }

      const updatedContact = await Contact.findOne({
        _id: contactId,
        created_by: userId,
        deleted_at: null
      }).lean();

      return {
        success: true,
        output: {
          ...inputData,
          contact: updatedContact,
          contactId: updatedContact?._id?.toString() || contactId,
          contact_updated: finalUpdates
        }
      };
    } catch (err) {
      console.error(`[Update Contact] Error:`, err);
      return { success: false, output: inputData, error: err.message };
    }
  }

  async executeAddToSegmentNode(node, inputData) {
    const { segment_id } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'add_to_segment', segment_id });
      return { success: true, output: inputData };
    }
    const contactId = inputData.contactId || inputData.contact?._id;
    const userId = inputData.userId || inputData.user_id;

    if (!segment_id) {
      return { success: false, output: inputData, error: 'segment_id is required' };
    }
    if (!contactId) {
      return { success: false, output: inputData, error: 'contactId is required' };
    }

    try {
      const { Segment, Contact } = await import('../models/index.js');
      const segment = await Segment.findOne({ _id: segment_id, user_id: userId, deleted_at: null });

      if (!segment) {
        return { success: false, output: inputData, error: 'Segment not found' };
      }

      await Contact.findByIdAndUpdate(contactId, {
        $addToSet: { segments: segment._id }
      });

      const count = await Contact.countDocuments({
        segments: segment._id,
        deleted_at: null
      });
      await Segment.findByIdAndUpdate(segment._id, { member_count: count });

      return {
        success: true,
        output: {
          ...inputData,
          last_segment_added: segment.name
        }
      };
    } catch (error) {
      console.error(`[Add to Segment] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }


  async executeResponseSaverNode(node, inputData) {
    const { mappings } = node.parameters || {};

    if (!Array.isArray(mappings) || mappings.length === 0) {
      return { success: true, output: inputData };
    }

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'response_saver', mappings });
      return { success: true, output: inputData };
    }

    const userId = inputData.userId || inputData.user_id;
    const contactId = inputData.contactId || inputData.contact?._id;

    if (!userId) {
      return { success: false, output: inputData, error: 'User ID is required to save responses' };
    }
    if (!contactId) {
      return { success: false, output: inputData, error: 'contactId is required to save responses' };
    }

    const customFieldUpdates = {};
    const variableOutputs = {};

    for (const mapping of mappings) {
      if (!mapping) continue;
      const { source_path, variable_name, custom_field_key } = mapping;
      if (!source_path) continue;

      const value = this.getNestedValue(inputData, source_path);

      if (variable_name) {
        variableOutputs[variable_name] = value;
      }

      if (custom_field_key) {
        customFieldUpdates[`custom_fields.${custom_field_key}`] = value;
      }
    }

    try {
      if (Object.keys(customFieldUpdates).length > 0) {
        await Contact.updateOne(
          { _id: contactId, created_by: userId, deleted_at: null },
          { $set: customFieldUpdates }
        );
      }

      const updatedContact = await Contact.findOne({
        _id: contactId,
        created_by: userId,
        deleted_at: null
      }).lean();

      return {
        success: true,
        output: {
          ...inputData,
          ...variableOutputs,
          contact: updatedContact || inputData.contact,
          contactId: updatedContact?._id?.toString() || contactId,
          response_saved: Object.keys(customFieldUpdates)
        }
      };
    } catch (err) {
      return { success: false, output: inputData, error: err.message };
    }
  }


  async executeApiNode(node, inputData) {
    const { url, method, headers, body, response_mapping } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'api', url, method });
      return { success: true, output: inputData };
    }

    if (!url) {
      console.error('[API Node] Error: URL is missing');
      return { success: false, output: inputData, error: 'API URL is required' };
    }

    try {
      const processedUrl = this.processTemplateString(url, inputData);
      const processedHeaders = this.processHeaders(headers || {}, inputData);
      const processedBody = body ? this.processTemplateString(JSON.stringify(body), inputData) : null;

      console.log(`[API Node] ${method || 'GET'} Request to: ${processedUrl}`);

      const response = await fetch(processedUrl, {
        method: method || 'GET',
        headers: processedHeaders,
        body: processedBody && method !== 'GET' ? processedBody : undefined
      });

      const responseText = await response.text();
      const apiResponse = this.isJsonString(responseText) ? JSON.parse(responseText) : responseText;

      console.log(`[API Node] Response Status: ${response.status} (${response.ok ? 'OK' : 'FAIL'})`);

      const userId = inputData.userId || inputData.user_id;
      const contactId = inputData.contactId || inputData.contact?._id;

      const variableOutputs = {};
      const customFieldUpdates = {};

      if (Array.isArray(response_mapping)) {
        for (const mapping of response_mapping) {
          if (!mapping) continue;
          const { response_path, variable_name, custom_field_key } = mapping;
          if (!response_path) continue;

          const value = this.getNestedValue(apiResponse, response_path);

          if (variable_name) {
            variableOutputs[variable_name] = value;
          }

          if (custom_field_key) {
            customFieldUpdates[`custom_fields.${custom_field_key}`] = value;
          }
        }
      }

      let updatedContact = inputData.contact;
      if (userId && contactId && Object.keys(customFieldUpdates).length > 0) {
        await Contact.updateOne(
          { _id: contactId, created_by: userId, deleted_at: null },
          { $set: customFieldUpdates }
        );

        updatedContact = await Contact.findOne({
          _id: contactId,
          created_by: userId,
          deleted_at: null
        }).lean();
      }

      return {
        success: response.ok,
        output: {
          ...inputData,
          ...variableOutputs,
          contact: updatedContact,
          contactId: updatedContact?._id?.toString() || contactId,
          api_status: response.status,
          api_response: apiResponse
        },
        ...(!response.ok ? { error: `API returned ${response.status}: ${typeof apiResponse === 'object' ? JSON.stringify(apiResponse) : apiResponse}` } : {})
      };
    } catch (error) {
      console.error(`[API Node] Runtime Error: ${error.message}`);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeSendProductNode(node, inputData) {
    const { product_id, recipient, provider_type } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: node.type || 'send_product', product_id });
      return { success: true, output: inputData };
    }

    if (!product_id) {
      return { success: false, output: inputData, error: 'Product ID is required' };
    }

    try {
      let processedRecipient = recipient ? this.processTemplateString(recipient, inputData) : null;
      const userId = inputData.userId || inputData.user_id;

      if (!userId) {
        return { success: false, output: inputData, error: 'User ID is required' };
      }

      const { EcommerceProduct, Contact, Message } = await import('../models/index.js');

      let contactDoc = null;
      const contactId = inputData.contactId || inputData.contact?._id;
      if (contactId) {
        contactDoc = await Contact.findOne({ _id: contactId, created_by: userId, deleted_at: null }).lean();
      } else if (processedRecipient) {
        contactDoc = await Contact.findOne({
          $or: [
            { phone_number: processedRecipient },
            { telegram_chat_id: processedRecipient },
            { facebook_page_scoped_id: processedRecipient },
            { instagram_scoped_id: processedRecipient }
          ],
          created_by: userId,
          deleted_at: null
        }).lean();
      }

      if (!contactDoc && !processedRecipient) {
        processedRecipient = inputData.senderNumber || inputData.phone_number || inputData.recipientNumber;
      } else if (contactDoc && !processedRecipient) {
        processedRecipient = contactDoc.phone_number || contactDoc.telegram_chat_id || contactDoc.instagram_scoped_id || contactDoc.facebook_page_scoped_id;
      }

      if (!contactDoc && !processedRecipient) {
        return { success: false, output: inputData, error: 'Recipient could not be determined' };
      }

      const platform = contactDoc ? contactDoc.source : 'whatsapp';

      const processedProductId = this.processTemplateString(product_id, inputData);
      const product = await EcommerceProduct.findById(processedProductId).lean();

      if (!product) {
        return { success: false, output: inputData, error: `Product not found: ${processedProductId}` };
      }

      const source = product.type || 'whatsapp';
      if (source === 'whatsapp' && platform !== 'whatsapp') {
        return { success: false, output: inputData, error: `Catalog products can only be sent on WhatsApp. Target platform: ${platform}` };
      }

      const product_name = product.name || 'Product Details';
      const product_price = product.price ? (product.currency ? `${product.currency} ${product.price}` : product.price) : 'N/A';
      const product_description = product.description || [
        product.brand ? `Brand: ${product.brand}` : '',
        product.category ? `Category: ${product.category}` : ''
      ].filter(Boolean).join(' | ');
      const product_image = product.image_urls && product.image_urls.length > 0 ? product.image_urls[0] : null;
      const product_url = product.url || 'https://shopify.com';

      const messageCaption = `*${product_name}*\n*Price:* ${product_price}${product_description ? `\n\n${product_description}` : ''}`;

      const productButton = {
        type: 'url',
        text: source === 'whatsapp' || source === 'catalog' ? 'View Catalog' : 'View Product',
        url: product_url
      };

      let resolvedMediaUrl = null;
      if (product_image) {
        resolvedMediaUrl = businessApiProvider.getPublicMediaUrl(product_image);
      }

      let lastResult = null;

      if (platform === 'whatsapp' || !contactDoc) {
        const messageParams = {
          recipientNumber: processedRecipient,
          messageType: 'interactive',
          interactiveType: 'cta_url',
          messageText: messageCaption.trim(),
          providerType: provider_type || PROVIDER_TYPES.BUSINESS_API,
          buttons: [productButton],
          buttonParams: { display_text: productButton.text, url: productButton.url }
        };

        if (resolvedMediaUrl) {
          messageParams.mediaUrl = resolvedMediaUrl;
          messageParams.file = {
            originalname: 'media',
            mimetype: this.getMimeTypeFromUrl(resolvedMediaUrl),
            buffer: null,
            url: resolvedMediaUrl
          };
        }

        if (inputData.whatsappPhoneNumberId) {
          const { WhatsappPhoneNumber } = await import('../models/index.js');
          const whatsappPhoneNumber = await WhatsappPhoneNumber.findById(inputData.whatsappPhoneNumberId).populate('waba_id').lean();
          if (whatsappPhoneNumber && whatsappPhoneNumber.waba_id) {
            messageParams.whatsappPhoneNumber = whatsappPhoneNumber;
          }
        } else if (inputData.whatsappConnectionId) {
          messageParams.connectionId = inputData.whatsappConnectionId;
        }

        lastResult = await unifiedWhatsAppService.sendMessage(userId, messageParams);
      } else if (platform === 'telegram') {
        const { default: TelegramConnection } = await import('../models/telegram-connection.model.js');
        const bot = await TelegramConnection.findOne({ user_id: userId, is_active: true });
        if (!bot) throw new Error('No active Telegram bot found for user');

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');

        let msgType = 'text';
        if (resolvedMediaUrl) {
          const extension = resolvedMediaUrl.split('.').pop().toLowerCase().split('?')[0];
          if (['jpg', 'jpeg', 'png', 'webp', 'gif'].includes(extension)) msgType = 'image';
          else msgType = 'document';
        }

        const result = await omnichannelService.sendMessage({
          platform: 'telegram',
          workspace_id: bot.workspace_id,
          user_id: userId,
          recipient_id: contactDoc.telegram_chat_id,
          message_type: msgType,
          text: messageCaption.trim(),
          file_url: resolvedMediaUrl,
          buttons: [productButton]
        });

        const telegramMsgId = result?.message_id?.toString() || `tg_${Date.now()}`;

        const tgInteractiveData = {
          interactiveType: 'button',
          buttons: [productButton]
        };

        const newMessage = await Message.create({
          workspace_id: bot.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: 'telegram',
          provider: 'telegram',
          sender_id: bot.bot_id,
          recipient_id: contactDoc.telegram_chat_id,
          direction: 'outbound',
          message_type: msgType,
          content: messageCaption.trim() || 'Product Details',
          file_url: resolvedMediaUrl,
          interactive_data: tgInteractiveData,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          ioInstance.emit('whatsapp:message', {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: { id: bot.bot_id, name: bot.bot_id },
            recipient: { id: contactDoc.telegram_chat_id, name: contactDoc.name },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: 'telegram',
            provider: 'telegram'
          });
        }

        lastResult = { provider: 'telegram', messageId: telegramMsgId };
      } else if (platform === 'facebook' || platform === 'instagram') {
        let connection = null;
        if (platform === 'facebook') {
          const { default: FacebookConnection } = await import('../models/facebook-connection.model.js');
          connection = await FacebookConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) connection = await FacebookConnection.findOne({ user_id: userId, is_active: true });
        } else {
          const { default: InstagramConnection } = await import('../models/instagram-connection.model.js');
          connection = await InstagramConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) connection = await InstagramConnection.findOne({ user_id: userId, is_active: true });
        }

        if (!connection) throw new Error(`No active ${platform} connection found for user`);

        let pageId = null;
        const lastMsg = await Message.findOne({ contact_id: contactDoc._id }).sort({ wa_timestamp: -1 }).lean();
        if (lastMsg) pageId = lastMsg.direction === 'inbound' ? lastMsg.recipient_id : lastMsg.sender_id;

        if (platform === 'facebook') {
          if (!pageId) {
            const page = connection.pages?.find(p => p.is_active !== false);
            pageId = page?.page_id;
          }
          if (!pageId) throw new Error(`No active Facebook Page found`);
        } else {
          if (!pageId) pageId = connection.ig_user_id || (connection.pages && connection.pages[0]?.instagram_account_id);
          if (!pageId) throw new Error(`No active Instagram Account found`);
        }

        const recipientId = platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id;
        if (!recipientId) throw new Error(`Recipient scoped ID not found`);

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');

        let msgType = 'text';
        if (resolvedMediaUrl) {
          const extension = resolvedMediaUrl.split('.').pop().toLowerCase().split('?')[0];
          if (['jpg', 'jpeg', 'png', 'webp', 'gif'].includes(extension)) msgType = 'image';
          else msgType = 'document';
        }

        const result = await omnichannelService.sendMessage({
          platform,
          workspace_id: connection.workspace_id,
          user_id: userId,
          page_id: pageId,
          recipient_id: recipientId,
          message_type: msgType,
          text: messageCaption.trim(),
          file_url: resolvedMediaUrl,
          buttons: [productButton]
        });

        const fbIgMsgId = result?.message_id?.toString() || `fbig_${Date.now()}`;

        const fbIgInteractiveData = {
          interactiveType: 'button',
          buttons: [productButton]
        };

        const newMessage = await Message.create({
          workspace_id: connection.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: platform,
          provider: platform,
          sender_id: pageId,
          recipient_id: recipientId,
          direction: 'outbound',
          message_type: msgType,
          content: messageCaption.trim() || 'Product Details',
          file_url: resolvedMediaUrl,
          interactive_data: fbIgInteractiveData,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          ioInstance.emit('whatsapp:message', {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: { id: pageId, name: pageId },
            recipient: { id: recipientId, name: contactDoc.name },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: platform,
            provider: platform
          });
        }

        lastResult = { provider: platform, messageId: fbIgMsgId };
      }

      return {
        success: true,
        output: {
          ...inputData,
          product_sent: true,
          sent_to: processedRecipient,
          provider: lastResult ? lastResult.provider : null,
          message_id: lastResult ? lastResult.messageId : null
        }
      };

    } catch (error) {
      console.error('Error in executeSendProductNode:', error);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeCustomNode(node, inputData) {
    const { custom_logic, parameters } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'custom', custom_logic });
      return { success: true, output: inputData };
    }

    console.log('Executing custom node:', custom_logic);

    if (custom_logic === 'update_order_status') {
      const userId = inputData.userId || inputData.user_id;
      const orderId = this.processTemplateString(parameters?.order_id || '', inputData);
      const status = parameters?.status;

      if (!userId) {
        return { success: false, output: inputData, error: 'User ID is required to update order status' };
      }
      if (!orderId) {
        return { success: false, output: inputData, error: 'order_id is required' };
      }
      if (!status) {
        return { success: false, output: inputData, error: 'status is required' };
      }

      const updated = await EcommerceOrder.findOneAndUpdate(
        { _id: orderId, user_id: userId, deleted_at: null },
        { $set: { status } },
        { returnDocument: 'after' }
      ).lean();

      return {
        success: !!updated,
        output: { ...inputData, order: updated, order_status_updated: status },
        ...(updated ? {} : { error: 'Order not found' })
      };
    }

    return {
      success: true,
      output: { ...inputData, custom_executed: true }
    };
  }

  async executeMoveToPipelineStageNode(node, inputData) {
    const { funnel_id, stage_id } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'move_to_pipeline_stage', funnel_id, stage_id });
      return { success: true, output: inputData };
    }

    const userId = inputData.userId || inputData.user_id;
    const contactId = inputData.contactId || inputData.contact?._id;

    if (!userId) {
      return { success: false, output: inputData, error: 'User ID is required to move pipeline stage' };
    }
    if (!contactId) {
      return { success: false, output: inputData, error: 'Contact ID is required to move pipeline stage' };
    }
    if (!funnel_id || !stage_id) {
      return { success: false, output: inputData, error: 'funnel_id and stage_id are required' };
    }

    try {
      const { KanbanFunnel, KanbanItem, Contact } = await import('../models/index.js');

      const funnel = await KanbanFunnel.findOne({ _id: funnel_id, userId, deletedAt: null });
      if (!funnel) {
        return { success: false, output: inputData, error: 'Funnel not found' };
      }

      const targetStage = funnel.stages.find(s => s._id.toString() === stage_id.toString());
      if (!targetStage) {
        return { success: false, output: inputData, error: 'Stage not found in funnel' };
      }

      const contactDoc = await Contact.findOne({ _id: contactId, created_by: userId, deleted_at: null }).lean();
      if (!contactDoc) {
        return { success: false, output: inputData, error: 'Contact not found' };
      }

      let item = await KanbanItem.findOne({ globalItemId: contactId, funnelId: funnel_id });
      let isNewItem = false;
      let fromStageId = null;

      if (item) {
        fromStageId = item.stageId;
        item.stageId = stage_id;
        item.movedAt = new Date();
      } else {
        isNewItem = true;

        const mapData = (itemData) => {
          let title = itemData.name || `${itemData.first_name || ''} ${itemData.last_name || ''}`.trim() || 'No Name';
          let subtitle = itemData.email || itemData.phone_number || 'No Contact Info';
          let label = 'Contact';
          let extra = {
            phone: itemData.phone_number || itemData.phone,
            company: itemData.company,
            source: itemData.source,
            chat_status: itemData.chat_status
          };

          const cleanedExtra = Object.fromEntries(
            Object.entries(extra).filter(([_, v]) => v !== null && v !== undefined)
          );

          return { title, subtitle, label, extra: cleanedExtra };
        };

        const mappedData = mapData(contactDoc);

        item = new KanbanItem({
          funnelId: funnel_id,
          globalItemId: contactId,
          stageId: stage_id,
          data: mappedData,
          history: []
        });
      }

      item.history.push({
        action: isNewItem ? 'created' : 'moved',
        fromStageId,
        toStageId: stage_id,
        timestamp: new Date(),
        details: 'Moved by automation'
      });

      await item.save();

      return {
        success: true,
        output: {
          ...inputData,
          pipeline_stage_moved: true,
          funnel_name: funnel.name,
          stage_name: targetStage.name
        }
      };
    } catch (err) {
      console.error('[Move to Pipeline Stage] Error:', err);
      return { success: false, output: inputData, error: err.message };
    }
  }


  async updateFlowStatistics(flowId, success) {
    try {
      const update = {
        $inc: {
          'statistics.total_executions': 1,
          'statistics.average_execution_time': 0
        }
      };

      if (success) {
        update.$inc['statistics.successful_executions'] = 1;
      } else {
        update.$inc['statistics.failed_executions'] = 1;
      }

      update.$set = { 'statistics.last_execution': new Date() };

      await AutomationFlow.findByIdAndUpdate(flowId, update);
    } catch (error) {
      console.error('Error updating flow statistics:', error);
    }
  }


  async executeWaitForReplyNode(node, flow, inputData, executionLog) {
    const { variable_name = 'last_user_message' } = node.parameters || {};

    const nextNodes = this.getConnectedNodes(flow, node.id);
    const nextNodeId = nextNodes.length > 0 ? nextNodes[0].id : null;

    const executionId = inputData.executionId;
    if (!executionId) {
      return { success: false, error: 'Could not find active execution' };
    }

    // Clear resume flag before persisting so the next resume starts without it
    const inputDataToSave = { ...inputData };
    delete inputDataToSave.is_resuming;

    await AutomationExecution.findByIdAndUpdate(executionId, {
      status: 'waiting',
      next_node_id: nextNodeId,
      waiting_for_node_id: node.id,
      contact_identifier: inputData.senderNumber,
      input_data: inputDataToSave
    });

    console.log(`Execution ${executionId} is now waiting for reply from ${inputData.senderNumber}. Next node: ${nextNodeId}`);

    return { success: true, status: 'waiting', output: inputData };
  }

  async executeAskAndWaitNode(node, flow, inputData, executionLog) {
    const { timeout_minutes = 10, recipient } = node.parameters || {};

    const simulatedNode = { ...node, parameters: { ...(node.parameters || {}) } };
    if (!simulatedNode.parameters.recipient) {
      simulatedNode.parameters.recipient = inputData.senderNumber || inputData.phone_number || '{{senderNumber}}';
    }
    // Map question_message / message → message_body so executeSendMessageNode finds the text
    if (!simulatedNode.parameters.message_body) {
      simulatedNode.parameters.message_body =
        simulatedNode.parameters.question_message ||
        simulatedNode.parameters.message ||
        simulatedNode.parameters.bodyText ||
        simulatedNode.parameters.text ||
        '';
    }

    const sendResult = await this.executeSendMessageNode(simulatedNode, inputData);
    if (!sendResult.success && !inputData.is_test) {
      // Log the error but still proceed to set up the wait so flow doesn't terminate
      console.error(`[Ask and Wait] Failed to send question message: ${sendResult.error}. Proceeding to wait state anyway.`);
    }

    const executionId = inputData.executionId;
    if (!executionId) {
      return { success: false, error: 'Could not find active execution' };
    }

    // Determine next node so free-text captures can resume correctly
    const nextNodes = this.getConnectedNodes(flow, node.id);
    const expectedAnswers = node.parameters?.expected_answers || [];
    const hasExpectedAnswers = Array.isArray(expectedAnswers) && expectedAnswers.length > 0;
    const nextNodeId = (!hasExpectedAnswers && nextNodes.length > 0) ? nextNodes[0].id : null;

    let timeoutAt = null;
    if (timeout_minutes > 0) {
      timeoutAt = new Date();
      timeoutAt.setMinutes(timeoutAt.getMinutes() + Number(timeout_minutes));
    }

    const contactIdentifier = simulatedNode.parameters.recipient.includes('{{')
      ? this.processTemplateString(simulatedNode.parameters.recipient, inputData)
      : simulatedNode.parameters.recipient;

    // Clear resume flag before persisting so the next resume starts without it
    const inputDataToSave = { ...inputData };
    delete inputDataToSave.is_resuming;

    await AutomationExecution.findByIdAndUpdate(executionId, {
      status: 'waiting',
      waiting_for_node_id: node.id,
      next_node_id: nextNodeId,
      contact_identifier: contactIdentifier,
      input_data: inputDataToSave,
      timeout_at: timeoutAt,
      retry_count: 0
    });

    console.log(`Execution ${executionId} is waiting for adaptive input (ask_and_wait). Next node: ${nextNodeId}. Timeout at: ${timeoutAt}`);

    return { success: true, status: 'waiting', output: inputData };
  }


  findExecutionIdByLog(executionLog) {
    for (const [key, value] of this.runningExecutions.entries()) {
      return value;
    }
    return null;
  }


  async resumeExecution(flow, execution, eventData, messagePayload) {
    console.log(`Resuming automation execution ${execution._id}`);

    const waitingNode = flow.nodes.find(n => n.id === execution.waiting_for_node_id);

    if (waitingNode && eventData.interactive_id) {
      const matchedRoute = eventData.interactive_id;
      if (matchedRoute.includes('___')) {
        const parts = matchedRoute.split('___');
        if (parts.length >= 3) {
          const buttonNodeId = parts[1];
          if (buttonNodeId !== waitingNode.id) {
            console.log(`[Resume Execution] Button/List clicked belongs to node "${buttonNodeId}", but waiting for node "${waitingNode.id}". Skipping resume.`);
            return { success: false, notMatched: true };
          }
        }
      }
    }

    const variableName = waitingNode?.parameters?.variable_name || 'last_user_message';

    // --- Resolve human-readable button/list label ---
    // Priority: textContent (button label from platform) > fallback button param lookup > raw interactive_id > plain message
    const rawInteractiveId = eventData.interactive_id || null;
    let resolvedLabel = eventData.textContent || null;

    // If no textContent but we have an interactive_id, try to resolve the label from the waiting node's button/list params
    if (!resolvedLabel && rawInteractiveId && waitingNode) {
      const cleanSystemPrefixes = (val) => {
        if (typeof val !== 'string') return val;
        let cur = val, prev;
        do { prev = cur; cur = cur.replace(/^(f[0-9a-f]{6}|new[a-z0-9]+)___/i, ''); } while (cur !== prev);
        return cur;
      };
      const cleanIncoming = cleanSystemPrefixes(rawInteractiveId).toLowerCase();

      const buttonParams = waitingNode.parameters?.button_params || waitingNode.parameters?.buttons || [];
      for (let i = 0; i < buttonParams.length; i++) {
        const btn = buttonParams[i];
        const rawId = btn.id || btn.value || `btn_${i + 1}`;
        const cleanId = cleanSystemPrefixes(rawId.toString()).toLowerCase();
        if (cleanId === cleanIncoming || rawId.toString().toLowerCase() === rawInteractiveId.toLowerCase()) {
          resolvedLabel = btn.title || btn.text || null;
          break;
        }
      }

      // Check list items as well
      if (!resolvedLabel) {
        const items = waitingNode.parameters?.list_params?.items || [];
        for (let i = 0; i < items.length; i++) {
          const item = items[i];
          const rawId = item.id || item.title || `item_${i + 1}`;
          const cleanId = cleanSystemPrefixes(rawId.toString()).toLowerCase();
          if (cleanId === cleanIncoming || rawId.toString().toLowerCase() === rawInteractiveId.toLowerCase()) {
            resolvedLabel = item.title || null;
            break;
          }
        }
      }
    }

    // messageText used for condition evaluation = human-readable label when available
    const rawMessage = rawInteractiveId || eventData.message;
    const rawText = typeof rawMessage === 'string' ? rawMessage : rawMessage?.text?.body || '';
    // Use the human-readable label for condition checks; fall back to raw text only if nothing else resolved
    let messageText = resolvedLabel || rawText;

    console.log(`[Resume] waitingNode="${waitingNode?.type}" rawInteractiveId="${rawInteractiveId}" resolvedLabel="${resolvedLabel}" messageText="${messageText}"`);

    let currentData = {
      ...execution.input_data,
      executionId: execution._id,
      test_responses: [],
      [variableName]: messageText,
      last_message: messageText,
      // 'message' must be the human-readable label so Logic Control conditions like
      // "message equals Tomorrow" work correctly regardless of the technical button ID
      message: messageText,
      messagePayload,
      flow_id: flow._id,
      textContent: resolvedLabel || eventData.textContent || null,
      interactive_id: rawInteractiveId,
      // Signal that we are mid-resume so processConnectedNodes skips auto-wait
      // (prevents Send Message branches from pausing again when flowing to ask_and_wait)
      is_resuming: true
    };

    if (eventData.form_data) {
      currentData = { ...currentData, ...eventData.form_data };
    }

    let nextNodeId = execution.next_node_id;

    // Auto-wait case: waitingNode is send_message, next is a condition node.
    // Inject the user's reply into every field referenced by the condition rules
    // so the condition can evaluate correctly (e.g. field "Plan" gets value "Premium").
    if (waitingNode && waitingNode.type === 'send_message' && nextNodeId) {
      const conditionNode = flow.nodes.find(n => n.id === nextNodeId);
      if (conditionNode && (conditionNode.type === 'advanced_condition' || conditionNode.type === 'condition')) {
        const rules = conditionNode.parameters?.rules || [];
        const conditions = conditionNode.parameters?.conditions || [];
        const allFields = [
          ...rules.map(r => r.field),
          ...conditions.map(c => c.field),
          conditionNode.parameters?.condition?.field
        ].filter(f => f && typeof f === 'string' && f !== 'message');

        allFields.forEach(fieldName => {
          currentData[fieldName] = messageText;
          currentData[fieldName.toLowerCase()] = messageText;
        });

        currentData.__auto_wait_reply = messageText;

        console.log(`[Auto-wait Resume] Injected user reply "${messageText}" into condition fields: [${allFields.join(', ')}]`);
      }
    }

    if (waitingNode && waitingNode.type === 'ask_and_wait') {
      const { expected_answers, validation, fallback_route, retry_limit = 2, retry_message } = waitingNode.parameters || {};
      let matchedRoute = null;
      // Track whether the node has any expected_answers configured (button/list mode)
      const hasExpectedAnswers = Array.isArray(expected_answers) && expected_answers.length > 0;

      const cleanSystemPrefixes = (val) => {
        if (typeof val !== 'string') return val;
        let current = val;
        let previous;
        do {
          previous = current;
          current = current.replace(/^(f[0-9a-f]{6}|new[a-z0-9]+)___/i, '');
        } while (current !== previous);
        return current;
      };

      if (hasExpectedAnswers) {
        const match = expected_answers.find(a => {
          if (!a.value) return false;
          const targetVal = a.value.trim().toLowerCase();
          const rawIncomingVal = messageText.trim().toLowerCase();
          const incomingVal = cleanSystemPrefixes(rawIncomingVal);

          if (incomingVal === targetVal) return true;
          if (rawIncomingVal.includes('___')) {
            const parts = rawIncomingVal.split('___');
            if (parts[parts.length - 1] === targetVal) return true;
            if (parts[parts.length - 1] === `btn_${expected_answers.indexOf(a) + 1}`) return true;
          }
          const btnTitle = eventData.textContent || eventData.message;
          if (btnTitle && typeof btnTitle === 'string' && btnTitle.trim().toLowerCase() === targetVal) return true;
          return false;
        });
        if (match) {
          matchedRoute = match.route;
        }
      }

      let isValid = true;
      if (!matchedRoute && validation && validation.type) {
        if (validation.type === 'email') {
          isValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(messageText.trim());
        } else if (validation.type === 'number') {
          isValid = !isNaN(Number(messageText.trim()));
        } else if (validation.type === 'regex' && validation.regex) {
          try { isValid = new RegExp(validation.regex).test(messageText.trim()); } catch (e) { }
        }
      }

      if (!matchedRoute && !isValid) {
        const retryCount = (execution.retry_count || 0) + 1;
        if (retryCount <= retry_limit) {
          console.log(`[Smart Capture] Validation failed. Retry ${retryCount}/${retry_limit}`);

          if (retry_message && currentData.senderNumber) {
            const processedRetryMsg = this.processTemplateString(retry_message, currentData);

            if (currentData.is_test) {
              currentData.test_responses = currentData.test_responses || [];
              currentData.test_responses.push({
                node_id: waitingNode.id,
                type: 'send_message',
                message: processedRetryMsg,
                messageParams: {
                  messageType: 'text',
                  text: { body: processedRetryMsg }
                }
              });
            } else {
              // Send retry via the correct platform (use same path as executeSendMessageNode)
              const retryNode = {
                id: waitingNode.id,
                type: 'send_message',
                parameters: {
                  recipient: currentData.senderNumber,
                  message_body: processedRetryMsg,
                  messageType: 'text'
                }
              };
              await this.executeSendMessageNode(retryNode, currentData)
                .catch(e => console.error('Failed to send retry msg:', e));
            }
          }

          await AutomationExecution.findByIdAndUpdate(execution._id, { retry_count: retryCount, input_data: currentData });
          return { success: true, status: 'waiting', output: currentData, executionLog: execution.execution_log || [] };
        } else {
          matchedRoute = fallback_route || 'fallback';
        }
      }

      if (!matchedRoute && hasExpectedAnswers) {
        matchedRoute = fallback_route || 'fallback';
      }

      if (matchedRoute) {
        // Try explicit route connection first (for expected_answers button matches)
        let connection = flow.connections.find(c => c.source === waitingNode.id && c.sourceHandle === matchedRoute);
        if (!connection) {
          connection = flow.connections.find(c => {
            if (c.source !== waitingNode.id) return false;
            const isFallbackHandle = c.sourceHandle === 'fallback' || c.sourceHandle === 'fallback_route';
            const isExpectedAnswersHandle = c.sourceHandle === 'expected_answers' || c.sourceHandle === matchedRoute;

            if (matchedRoute === 'fallback' || matchedRoute === fallback_route) {
              return isFallbackHandle;
            } else {
              return isExpectedAnswersHandle;
            }
          });
        }
        if (connection) {
          nextNodeId = connection.target;
        } else {
          // Route not found — fall back to next_node_id set during ask_and_wait execution
          nextNodeId = execution.next_node_id || null;
        }
      }
      // If no matchedRoute (free-text capture with no expected_answers), use the default next node
      // already stored in execution.next_node_id when the ask_and_wait node paused execution.
      // nextNodeId was initialised to execution.next_node_id at line 3626, so no change needed here.
      console.log(`[ask_and_wait Resume] matchedRoute="${matchedRoute}" nextNodeId="${nextNodeId}" variableName="${variableName}" reply="${messageText}"`);
    } else if (waitingNode) {
      const matchedRoute = eventData.interactive_id || '';
      const incomingText = (matchedRoute || messageText || eventData.message || eventData.textContent || '').toString().trim().toLowerCase();
      const cleanRoute = matchedRoute ? matchedRoute.split('___').pop().toLowerCase() : incomingText;

      if (cleanRoute) {
        console.log(`[Resume Execution] Resolving reply: "${incomingText}" (clean key: "${cleanRoute}") for node type "${waitingNode.type}"`);

        let connection = null;

        // 1. Index-based matching for buttons
        if (waitingNode.type === 'send_message' || waitingNode.type === 'button_message') {
          const buttonParams = waitingNode.parameters?.button_params || waitingNode.parameters?.buttons || [];
          let matchedIndex = -1;
          for (let i = 0; i < buttonParams.length; i++) {
            const btn = buttonParams[i];
            const rawId = btn.id || btn.value || `btn_${i + 1}`;
            const title = btn.title || btn.text || '';
            const cleanRawId = rawId.toString().split('___').pop().toLowerCase();

            if (rawId.toString().toLowerCase() === cleanRoute ||
              cleanRawId === cleanRoute ||
              title.toString().toLowerCase() === cleanRoute ||
              title.toString().toLowerCase() === incomingText) {
              matchedIndex = i;
              break;
            }
          }
          if (matchedIndex !== -1) {
            const targetHandle = `src-btn-${matchedIndex}`;
            connection = flow.connections.find(c => c.source === waitingNode.id && c.sourceHandle === targetHandle);
            if (connection) {
              console.log(`[Resume Execution] Matched button index ${matchedIndex} to handle "${targetHandle}" -> target node "${connection.target}"`);
            } else {
              const virtualSourcePrefix = `cond-btn-___${waitingNode.id}___${matchedIndex}___`;
              connection = flow.connections.find(c => c.source && c.source.startsWith(virtualSourcePrefix));
              if (connection) {
                console.log(`[Resume Execution] Matched virtual button condition to target node "${connection.target}"`);
              }
            }
          }
        }

        // 2. Index-based matching for list items
        if (!connection && (waitingNode.type === 'send_message' || waitingNode.type === 'list_message')) {
          const sections = waitingNode.parameters?.list_params?.sections || [];
          let matchedSectionIdx = -1;
          let matchedItemIdx = -1;

          if (sections.length > 0) {
            for (let sIdx = 0; sIdx < sections.length; sIdx++) {
              const section = sections[sIdx];
              const rows = section.rows || section.items || [];
              for (let iIdx = 0; iIdx < rows.length; iIdx++) {
                const item = rows[iIdx];
                const rawId = item.id || item.rowId || `row_${iIdx}`;
                const title = item.title || '';
                const cleanRawId = rawId.toString().split('___').pop().toLowerCase();

                if (rawId.toString().toLowerCase() === cleanRoute ||
                  cleanRawId === cleanRoute ||
                  title.toString().toLowerCase() === cleanRoute ||
                  title.toString().toLowerCase() === incomingText) {
                  matchedSectionIdx = sIdx;
                  matchedItemIdx = iIdx;
                  break;
                }
              }
              if (matchedSectionIdx !== -1) break;
            }
          } else {
            const items = waitingNode.parameters?.list_params?.items || [];
            for (let iIdx = 0; iIdx < items.length; iIdx++) {
              const item = items[iIdx];
              const rawId = item.id || item.title || `item_${iIdx + 1}`;
              const title = item.title || '';
              const cleanRawId = rawId.toString().split('___').pop().toLowerCase();

              if (rawId.toString().toLowerCase() === cleanRoute ||
                cleanRawId === cleanRoute ||
                title.toString().toLowerCase() === cleanRoute ||
                title.toString().toLowerCase() === incomingText) {
                matchedSectionIdx = 0;
                matchedItemIdx = iIdx;
                break;
              }
            }
          }

          if (matchedSectionIdx !== -1 && matchedItemIdx !== -1) {
            const targetHandle = `src-item-${matchedSectionIdx}-${matchedItemIdx}`;
            connection = flow.connections.find(c => c.source === waitingNode.id && c.sourceHandle === targetHandle);
            if (connection) {
              console.log(`[Resume Execution] Matched list item section ${matchedSectionIdx}, item ${matchedItemIdx} to handle "${targetHandle}" -> target node "${connection.target}"`);
            } else {
              const virtualSourcePrefix = `cond-list-___${waitingNode.id}___${matchedSectionIdx}___${matchedItemIdx}___`;
              connection = flow.connections.find(c => c.source && c.source.startsWith(virtualSourcePrefix));
              if (connection) {
                console.log(`[Resume Execution] Matched virtual list item condition to target node "${connection.target}"`);
              }
            }
          }
        }

        // 3. Fallback to direct handle match
        if (!connection && matchedRoute) {
          connection = flow.connections.find(c => {
            if (c.source !== waitingNode.id) return false;
            if (!c.sourceHandle) return false;

            if (c.sourceHandle === matchedRoute) return true;
            if (c.sourceHandle.replace('src-', '').replace('-', '_') === matchedRoute) return true;
            if (c.sourceHandle.includes(matchedRoute) || matchedRoute.includes(c.sourceHandle)) return true;

            return false;
          });
        }

        if (connection) {
          nextNodeId = connection.target;
        }
      }
    }

    await AutomationExecution.findByIdAndUpdate(execution._id, {
      status: 'running',
      contact_identifier: null
    });

    if (!nextNodeId) {
      console.log('No next node to execute after resume');
      await AutomationExecution.findByIdAndUpdate(execution._id, {
        status: 'success',
        completed_at: new Date()
      });
      return { success: true, output: currentData, executionLog: [] };
    }

    const nextNode = flow.nodes.find(n => n.id === nextNodeId);
    if (!nextNode) {
      throw new Error(`Next node ${nextNodeId} not found in flow`);
    }

    const executionLog = execution.execution_log || [];
    const executionId = uuidv4();
    this.runningExecutions.set(executionId, execution._id);

    const startTime = Date.now();
    const nodeResult = await this.executeNode(nextNode, flow, currentData, executionLog);

    if (nodeResult.status === 'waiting') {
      this.runningExecutions.delete(executionId);
      return { success: true, status: 'waiting', output: currentData, executionLog };
    }

    if (nodeResult.success) {
      const updatedData = {
        ...currentData,
        ...nodeResult.output
      };


      const connResult = await this.processConnectedNodes(flow, nextNode, updatedData, executionLog, currentData);
      if (connResult && connResult.status === 'waiting') {
        this.runningExecutions.delete(executionId);
        return { success: true, status: 'waiting', output: connResult.output || updatedData, executionLog };
      }
      if (connResult && connResult.success) {
        currentData = connResult.output;
      } else {
        currentData = updatedData;
      }
    }

    const resultStatus = nodeResult.success ? 'success' : 'failed';
    const executionTime = Date.now() - startTime;

    await AutomationExecution.findByIdAndUpdate(execution._id, {
      status: resultStatus,
      output_data: currentData,
      execution_time: executionTime,
      completed_at: new Date(),
      execution_log: executionLog
    });

    await this.updateFlowStatistics(flow._id, nodeResult.success);
    this.runningExecutions.delete(executionId);

    return { success: true, output: currentData, executionLog };
  }


  async triggerEvent(eventType, eventData) {
    console.log('Triggering event:', eventType, 'with data:', eventData);
    const handler = this.eventListeners.get(eventType);
    if (handler) {
      console.log('Found handler for event:', eventType);
      await handler(eventData);
    } else {
      console.log('No handler found for event:', eventType, 'Available handlers:', Array.from(this.eventListeners.keys()));
    }
  }


  getRunningExecutions() {
    return Array.from(this.runningExecutions.values());
  }


  async cancelExecution(executionId) {
    if (this.runningExecutions.has(executionId)) {
      await AutomationExecution.findByIdAndUpdate(
        this.runningExecutions.get(executionId),
        { status: 'cancelled', completed_at: new Date() }
      );
      this.runningExecutions.delete(executionId);
    }
  }

  async executeAddTagNode(node, inputData) {
    const { tag_id, tag_name } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'add_tag', tag_id, tag_name });
      return { success: true, output: inputData };
    }
    const contactId = inputData.contactId;

    if (!contactId) {
      return { success: false, output: inputData, error: 'contactId is required' };
    }

    try {
      let tag;
      if (tag_id) {
        tag = await Tag.findById(tag_id);
      } else if (tag_name) {
        const userId = inputData.userId || inputData.user_id;
        tag = await Tag.findOne({ label: tag_name, created_by: userId, deleted_at: null });

        if (!tag) {
          tag = await Tag.create({
            label: tag_name,
            created_by: userId,
            color: '#007bff'
          });
        }
      }

      if (!tag) {
        return { success: false, output: inputData, error: 'Tag not found and could not be created' };
      }

      await Contact.findByIdAndUpdate(contactId, {
        $addToSet: { tags: tag._id }
      });

      await ContactTag.findOneAndUpdate(
        { contact_id: contactId, tag_id: tag._id },
        { deleted_at: null },
        { upsert: true }
      );

      console.log(`[add_tag] Added tag "${tag.label}" to contact ${contactId}`);
      return { success: true, output: { ...inputData, last_tag_added: tag.label } };
    } catch (error) {
      console.error(`[add_tag] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeRemoveTagNode(node, inputData) {
    const { tag_ids } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'remove_tag', tag_ids });
      return { success: true, output: inputData };
    }
    const contactId = inputData.contactId;

    if (!contactId) {
      return { success: false, output: inputData, error: 'contactId is required' };
    }

    try {
      let tagsToRemove = [];

      if (tag_ids && Array.isArray(tag_ids) && tag_ids.length > 0) {
        tagsToRemove = await Tag.find({ _id: { $in: tag_ids } });
      }

      if (tagsToRemove.length === 0) {
        return { success: true, output: inputData };
      }

      const tagIdsToRemove = tagsToRemove.map(t => t._id);
      const tagLabelsRemoved = tagsToRemove.map(t => t.label);

      await Contact.findByIdAndUpdate(contactId, {
        $pull: { tags: { $in: tagIdsToRemove } }
      });

      await ContactTag.updateMany(
        { contact_id: contactId, tag_id: { $in: tagIdsToRemove } },
        { deleted_at: new Date() }
      );

      console.log(`[remove_tag] Removed tags "${tagLabelsRemoved.join(', ')}" from contact ${contactId}`);
      return { success: true, output: { ...inputData, last_tags_removed: tagLabelsRemoved } };
    } catch (error) {
      console.error(`[remove_tag] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeAssignAgentNode(node, inputData) {
    const { agent_id } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'assign_agent', agent_id });
      return { success: true, output: inputData };
    }
    const senderNumber = inputData.senderNumber;
    const recipientNumber = inputData.recipientNumber;
    const whatsappPhoneNumberId = inputData.whatsappPhoneNumberId;
    const userId = inputData.userId || inputData.user_id;

    if (!agent_id) {
      return { success: false, output: inputData, error: 'agent_id is required' };
    }

    try {
      await ChatAssignment.findOneAndUpdate(
        {
          sender_number: senderNumber,
          receiver_number: recipientNumber,
          whatsapp_phone_number_id: whatsappPhoneNumberId
        },
        {
          agent_id,
          assigned_by: userId,
          status: 'assigned',
          is_solved: false
        },
        { upsert: true, returnDocument: 'after' }
      );

      console.log(`[assign_agent] Assigned human agent ${agent_id} to ${senderNumber}`);
      return { success: true, output: { ...inputData, agent_assigned_id: agent_id } };
    } catch (error) {
      console.error(`[assign_agent] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeAssignRandomAgentNode(node, inputData) {
    const { team_id } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'assign_random_agent', team_id });
      return { success: true, output: inputData };
    }
    const senderNumber = inputData.senderNumber;
    const recipientNumber = inputData.recipientNumber;
    const whatsappPhoneNumberId = inputData.whatsappPhoneNumberId;
    const userId = inputData.userId || inputData.user_id;

    try {
      const { User, Team } = await import('../models/index.js');
      let potentialAgents = [];
      let lastAgentId = null;
      let trackerUpdated = false;

      if (team_id) {
        const teamDoc = await Team.findById(team_id);
        if (teamDoc) {
          lastAgentId = teamDoc.round_robin_last_agent_id;
          potentialAgents = await User.find({ team_id: team_id, status: true, deleted_at: null })
            .sort({ created_at: 1 })
            .lean();
        }
      } else {
        const ownerDoc = await User.findById(userId);
        if (ownerDoc) {
          lastAgentId = ownerDoc.round_robin_last_agent_id;
          potentialAgents = await User.find({ created_by: userId, status: true, deleted_at: null })
            .sort({ created_at: 1 })
            .lean();

          if (ownerDoc.status === true && !ownerDoc.deleted_at) {
            potentialAgents.unshift(ownerDoc);
          }
        }
      }

      if (!potentialAgents || potentialAgents.length === 0) {
        return { success: false, output: inputData, error: 'No agents available for round-robin assignment' };
      }

      let nextIndex = 0;
      if (lastAgentId) {
        const lastIndex = potentialAgents.findIndex(a => a._id.toString() === lastAgentId.toString());
        if (lastIndex !== -1) {
          nextIndex = (lastIndex + 1) % potentialAgents.length;
        }
      }

      const selectedAgent = potentialAgents[nextIndex];
      const agent_id = selectedAgent._id;

      if (team_id) {
        await Team.findByIdAndUpdate(team_id, { round_robin_last_agent_id: agent_id });
      } else {
        await User.findByIdAndUpdate(userId, { round_robin_last_agent_id: agent_id });
      }

      await ChatAssignment.findOneAndUpdate(
        {
          sender_number: senderNumber,
          receiver_number: recipientNumber,
          whatsapp_phone_number_id: whatsappPhoneNumberId
        },
        {
          agent_id,
          assigned_by: userId,
          status: 'assigned',
          is_solved: false
        },
        { upsert: true, returnDocument: 'after' }
      );

      console.log(`[assign_random_agent] Randomly assigned human agent ${agent_id} to ${senderNumber}`);
      return { success: true, output: { ...inputData, agent_assigned_id: agent_id, team_assigned_id: team_id || null } };
    } catch (error) {
      console.error(`[assign_random_agent] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeSendCtaNode(node, inputData) {
    const { recipient, text, button_text, url } = node.parameters || {};

    const processedText = text ? this.processTemplateString(text, inputData) : '';
    const processedUrl = url ? this.processTemplateString(url, inputData) : '';
    const processedButtonText = button_text ? this.processTemplateString(button_text, inputData) : '';

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({
        node_id: node.id,
        type: 'cta_button',
        text: processedText,
        button_text: processedButtonText,
        url: processedUrl,
        messageParams: {
          messageText: processedText,
          messageType: 'interactive',
          buttonParams: [{ title: processedButtonText, id: processedUrl }]
        }
      });
      return { success: true, output: inputData };
    }
    const userId = inputData.userId || inputData.user_id;
    const whatsappPhoneNumberId = inputData.whatsappPhoneNumberId;

    if (!recipient || !text || !button_text || !url) {
      return { success: false, output: inputData, error: 'Missing required parameters for CTA button' };
    }

    const processedRecipient = this.processTemplateString(recipient, inputData);

    try {
      let contactDoc = null;
      const contactId = inputData.contactId || inputData.contact?._id;
      if (contactId) {
        contactDoc = await Contact.findOne({ _id: contactId, created_by: userId, deleted_at: null }).lean();
      } else {
        contactDoc = await Contact.findOne({
          $or: [
            { phone_number: processedRecipient },
            { telegram_chat_id: processedRecipient },
            { facebook_page_scoped_id: processedRecipient },
            { instagram_scoped_id: processedRecipient }
          ],
          created_by: userId,
          deleted_at: null
        }).lean();
      }

      if (contactDoc && contactDoc.source === 'telegram') {
        const { default: TelegramConnection } = await import('../models/telegram-connection.model.js');
        const bot = await TelegramConnection.findOne({ user_id: userId, is_active: true });
        if (!bot) {
          throw new Error('No active Telegram bot found for user');
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');
        const result = await omnichannelService.sendMessage({
          platform: 'telegram',
          workspace_id: bot.workspace_id,
          user_id: userId,
          recipient_id: contactDoc.telegram_chat_id,
          message_type: 'text',
          text: processedText,
          buttons: [
            {
              text: button_text,
              url: processedUrl,
              type: 'url'
            }
          ]
        });

        const telegramMsgId = result?.message_id?.toString() || `tg_${Date.now()}`;

        const newMessage = await Message.create({
          workspace_id: bot.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: 'telegram',
          provider: 'telegram',
          sender_id: bot.bot_id,
          recipient_id: contactDoc.telegram_chat_id,
          direction: 'outbound',
          message_type: 'interactive',
          content: processedText,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: bot.bot_id,
              name: bot.bot_id
            },
            recipient: {
              id: contactDoc.telegram_chat_id,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: 'telegram',
            provider: 'telegram'
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return { success: true, output: inputData };
      } else if (contactDoc && (contactDoc.source === 'facebook' || contactDoc.source === 'instagram')) {
        const platform = contactDoc.source;
        let connection = null;
        if (platform === 'facebook') {
          const { default: FacebookConnection } = await import('../models/facebook-connection.model.js');
          connection = await FacebookConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await FacebookConnection.findOne({ user_id: userId, is_active: true });
          }
        } else {
          const { default: InstagramConnection } = await import('../models/instagram-connection.model.js');
          connection = await InstagramConnection.findOne({ workspace_id: contactDoc.workspace_id || inputData.workspaceId, is_active: true });
          if (!connection) {
            connection = await InstagramConnection.findOne({ user_id: userId, is_active: true });
          }
        }

        if (!connection) {
          throw new Error(`No active ${platform} connection found for user`);
        }

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
          if (!pageId) throw new Error(`No active Facebook Page found for this connection`);
        } else {
          if (!pageId) {
            pageId = connection.ig_user_id || (connection.pages && connection.pages[0]?.instagram_account_id);
          }
          if (!pageId) throw new Error(`No active Instagram Account found for this connection`);
        }

        const recipientId = platform === 'facebook' ? contactDoc.facebook_page_scoped_id : contactDoc.instagram_scoped_id;
        if (!recipientId) {
          throw new Error(`Recipient scoped ID not found for contact`);
        }

        const { default: omnichannelService } = await import('../services/messaging/omnichannel.service.js');
        const result = await omnichannelService.sendMessage({
          platform,
          workspace_id: connection.workspace_id,
          user_id: userId,
          page_id: pageId,
          recipient_id: recipientId,
          message_type: 'text',
          text: `${processedText}\n\n${button_text}: ${processedUrl}`
        });

        const fbIgMsgId = result?.message_id?.toString() || `fbig_${Date.now()}`;

        const newMessage = await Message.create({
          workspace_id: connection.workspace_id,
          user_id: userId,
          contact_id: contactDoc._id,
          platform: platform,
          provider: platform,
          sender_id: pageId,
          recipient_id: recipientId,
          direction: 'outbound',
          message_type: 'text',
          content: `${processedText}\n\n${button_text}: ${processedUrl}`,
          from_me: true,
          delivery_status: 'delivered',
          read_status: 'unread',
          wa_timestamp: new Date()
        });

        const ioInstance = unifiedWhatsAppService.io;
        if (ioInstance) {
          const formattedMessage = {
            id: newMessage._id.toString(),
            content: newMessage.content,
            interactiveData: newMessage.interactive_data || null,
            messageType: newMessage.message_type,
            fileUrl: newMessage.file_url || null,
            createdAt: newMessage.wa_timestamp,
            can_chat: true,
            delivered_at: new Date(),
            delivery_status: newMessage.delivery_status || 'delivered',
            direction: newMessage.direction || 'outbound',
            sender: {
              id: pageId,
              name: pageId
            },
            recipient: {
              id: recipientId,
              name: contactDoc.name
            },
            user_id: newMessage.user_id?.toString(),
            contact_id: contactDoc._id.toString(),
            platform: platform,
            provider: platform
          };
          ioInstance.emit('whatsapp:message', formattedMessage);
        }

        return { success: true, output: inputData };
      }

      await unifiedWhatsAppService.sendMessage(userId, {
        recipientNumber: processedRecipient,
        whatsappPhoneNumberId,
        messageText: processedText,
        messageType: 'interactive',
        interactiveType: 'cta_url',
        buttonParams: {
          display_text: button_text,
          url: processedUrl
        }
      });

      return { success: true, output: inputData };
    } catch (error) {
      console.error(`[cta_button] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeAssignChatbotNode(node, inputData) {
    const {
      chatbot_id,
      duration_hours,
      tone,
      message_limit,
      enable_human_handoff,
      handoff_keywords,
      handoff_message
    } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'assign_chatbot', chatbot_id });
      return { success: true, output: inputData };
    }
    const senderNumber = inputData.senderNumber;
    const userId = inputData.userId || inputData.user_id;

    if (!chatbot_id) {
      return { success: false, output: inputData, error: 'chatbot_id is required' };
    }

    try {
      const { default: Contact } = await import('../models/contact.model.js');
      const contact = await Contact.findOne({
        $or: [{ phone_number: senderNumber }, { facebook_page_scoped_id: senderNumber }, { instagram_scoped_id: senderNumber }, { telegram_chat_id: senderNumber }],
        created_by: userId,
        deleted_at: null
      });

      if (!contact) {
        return { success: false, output: inputData, error: 'Contact not found' };
      }

      contact.assigned_chatbot = chatbot_id;
      contact.chatbot_paused = false;
      contact.ai_message_count = 0;

      const overrides = {};
      if (tone) overrides.tone = tone;
      if (message_limit !== undefined) overrides.message_limit = Number(message_limit);
      if (enable_human_handoff !== undefined) overrides.enable_human_handoff = enable_human_handoff;
      if (handoff_keywords) overrides.handoff_keywords = Array.isArray(handoff_keywords) ? handoff_keywords : handoff_keywords.split(',').map(k => k.trim());
      if (handoff_message) overrides.handoff_message = handoff_message;

      if (Object.keys(overrides).length > 0) {
        contact.chatbot_overrides = overrides;
      } else {
        contact.chatbot_overrides = null;
      }

      await contact.save();

      console.log(`[assign_chatbot] Assigned chatbot ${chatbot_id} to ${senderNumber} with overrides:`, overrides);
      return { success: true, output: { ...inputData, chatbot_assigned_id: chatbot_id } };
    } catch (error) {
      console.error(`[assign_chatbot] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }


  async executeSaveToGoogleSheetNode(node, inputData) {
    const { google_account_id, spreadsheet_id, sheet_name = 'Sheet1', column_mappings } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'save_to_google_sheet', spreadsheet_id });
      return { success: true, output: inputData };
    }

    if (!google_account_id || !spreadsheet_id) {
      return { success: false, output: inputData, error: 'google_account_id and spreadsheet_id are required' };
    }

    try {
      const sheets = await getSheetsClient(google_account_id);

      const spreadsheet = await sheets.spreadsheets.get({
        spreadsheetId: spreadsheet_id
      });

      const sheetNames = spreadsheet.data.sheets.map(s => s.properties.title);
      console.log(`[google_sheet] Available sheets in ${spreadsheet_id}:`, sheetNames);

      const targetSheet = sheetNames.find(name => name.trim() === sheet_name.trim()) || sheetNames[0];
      console.log(`[google_sheet] Using sheet: "${targetSheet}" (Requested: "${sheet_name}")`);

      let rowValues = [];
      if (Array.isArray(column_mappings) && column_mappings.length > 0) {
        rowValues = column_mappings.map(m => this.processTemplateString(m.value || '', inputData));
      } else {
        rowValues = [
          inputData.contact?.name || '',
          inputData.senderNumber || '',
          new Date().toLocaleString()
        ];
      }

      await sheets.spreadsheets.values.append({
        spreadsheetId: spreadsheet_id,
        range: `'${targetSheet}'`,
        valueInputOption: 'USER_ENTERED',
        requestBody: {
          values: [rowValues]
        }
      });

      return { success: true, output: { row_added: rowValues, sheet_used: targetSheet } };
    } catch (error) {
      console.error('Error saving to Google Sheet in automation:', error);
      await handleGoogleApiError(error, google_account_id);
      return { success: false, output: inputData, error: error.message };
    }
  }



  async executeSendGoogleFormNode(node, inputData) {
    const { google_account_id, form_id, message_body, recipient, provider_type } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'send_google_form', form_id });
      return { success: true, output: inputData };
    }

    if (!google_account_id || !form_id) {
      return { success: false, output: inputData, error: 'google_account_id and form_id are required' };
    }

    try {
      const forms = await getFormsClient(google_account_id);
      const formObj = await forms.forms.get({ formId: form_id });
      const responderUri = formObj.data.responderUri;

      let processedRecipient = recipient ? this.processTemplateString(recipient, inputData) : null;
      if (!processedRecipient) {
        processedRecipient = inputData.senderNumber || inputData.phone_number || inputData.recipientNumber;
      }

      if (!processedRecipient) {
        return { success: false, output: inputData, error: 'Recipient could not be determined' };
      }

      const processedMessage = this.processTemplateString(message_body || '', inputData);
      const finalMessage = processedMessage ? `${processedMessage}\n\n${responderUri}` : responderUri;

      const simulatedNode = {
        id: node.id,
        parameters: {
          recipient: processedRecipient,
          message_body: finalMessage,
          provider_type
        }
      };

      return await this.executeSendMessageNode(simulatedNode, inputData);
    } catch (error) {
      console.error('Error in executeSendGoogleFormNode:', error);
      await handleGoogleApiError(error, google_account_id);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeCreateCalendarEventNode(node, inputData) {
    const { google_account_id, calendar_id = 'primary', summary, description, start_time, end_time } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'create_calendar_event', summary });
      return { success: true, output: inputData };
    }

    console.log(`[google_calendar] Starting event creation for calendar: ${calendar_id}`);

    if (!google_account_id) {
      console.error('[google_calendar] Error: google_account_id is missing');
      return { success: false, output: inputData, error: 'google_account_id is required' };
    }

    try {
      const calendar = await getCalendarClient(google_account_id);

      const resolvedSummary = this.processTemplateString(summary || 'WhatsApp Scheduled Event', inputData);
      const resolvedDescription = this.processTemplateString(description || '', inputData);
      const resolvedStart = this.processTemplateString(start_time || new Date().toISOString(), inputData);

      console.log(`[google_calendar] Resolved Summary: "${resolvedSummary}"`);
      console.log(`[google_calendar] Resolved StartTime: "${resolvedStart}"`);

      let resolvedEnd = this.processTemplateString(end_time || '', inputData);
      if (!resolvedEnd) {
        const start = new Date(resolvedStart);
        start.setMinutes(start.getMinutes() + 30);
        resolvedEnd = start.toISOString();
      }

      const response = await calendar.events.insert({
        calendarId: calendar_id,
        requestBody: {
          summary: resolvedSummary,
          description: resolvedDescription,
          start: { dateTime: resolvedStart },
          end: { dateTime: resolvedEnd }
        }
      });

      console.log(`[google_calendar] Event created successfully: ${response.data.id}`);
      return { success: true, output: { event: response.data } };
    } catch (error) {
      console.error('[google_calendar] Error creating calendar event:', error.message);
      await handleGoogleApiError(error, google_account_id);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeCreateGoogleMeetNode(node, inputData) {
    const { google_account_id, calendar_id = 'primary', summary, description, start_time, end_time, variable_name = 'meet_link' } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'create_google_meet', summary });
      return { success: true, output: inputData };
    }

    console.log(`[google_meet] Starting event creation for calendar: ${calendar_id}`);

    if (!google_account_id) {
      return { success: false, output: inputData, error: 'google_account_id is required' };
    }

    try {
      const calendar = await getCalendarClient(google_account_id);

      const resolvedSummary = this.processTemplateString(summary || 'Google Meet Scheduled Event', inputData);
      const resolvedDescription = this.processTemplateString(description || '', inputData);
      const resolvedStart = this.processTemplateString(start_time || new Date().toISOString(), inputData);

      let resolvedEnd = this.processTemplateString(end_time || '', inputData);
      if (!resolvedEnd) {
        const start = new Date(resolvedStart);
        start.setMinutes(start.getMinutes() + 30);
        resolvedEnd = start.toISOString();
      }

      const response = await calendar.events.insert({
        calendarId: calendar_id,
        conferenceDataVersion: 1,
        requestBody: {
          summary: resolvedSummary,
          description: resolvedDescription,
          start: { dateTime: resolvedStart },
          end: { dateTime: resolvedEnd },
          conferenceData: {
            createRequest: {
              requestId: `meet_${Date.now()}_${Math.random().toString(36).substring(7)}`,
              conferenceSolutionKey: {
                type: 'hangoutsMeet'
              }
            }
          }
        }
      });

      console.log(`[google_meet] Event created successfully: ${response.data.id}`);
      const meetLink = response.data.hangoutLink || '';

      const output = { ...inputData, event: response.data };
      if (variable_name) {
        output[variable_name] = meetLink;
      }

      return { success: true, output };
    } catch (error) {
      console.error('[google_meet] Error creating calendar event:', error.message);
      await handleGoogleApiError(error, google_account_id);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeAppointmentFlowNode(node, flow, inputData, executionLog) {
    const { appointment_config_id } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'appointment_flow', appointment_config_id });
      return { status: 'waiting', success: true, output: inputData };
    }

    if (!appointment_config_id) {
      return { success: false, output: inputData, error: 'appointment_config_id is required' };
    }

    try {
      await appointmentService.startConversationalFlow({
        userId: flow.user_id,
        contactId: inputData.contactId,
        configId: appointment_config_id,
        whatsappPhoneNumberId: inputData.whatsappPhoneNumberId,
        inputData: inputData
      });

      return { status: 'waiting', success: true, output: inputData };
    } catch (error) {
      console.error('Error executing appointment flow node:', error);
      return { success: false, output: inputData, error: error.message };
    }
  }

  async executeSendSequenceNode(node, inputData) {
    const { sequence_id } = node.parameters || {};

    if (inputData.is_test) {
      inputData.test_responses = inputData.test_responses || [];
      inputData.test_responses.push({ node_id: node.id, type: 'send_sequence', sequence_id });
      return { success: true, output: inputData };
    }

    if (!sequence_id) {
      return { success: false, output: inputData, error: 'sequence_id is required' };
    }

    const contactId = inputData.contactId || inputData.contact?._id;
    const userId = inputData.userId || inputData.user_id;

    if (!contactId) {
      return { success: false, output: inputData, error: 'contactId is required to enroll in sequence' };
    }

    try {
      await handleSequenceReply({
        wabaId: inputData.contact?.waba_id || inputData.waba_id || null,
        contactId,
        sequenceId: sequence_id,
        userId,
        whatsappPhoneNumberId: inputData.whatsappPhoneNumberId
      });

      console.log(`[send_sequence] Enrolled contact ${contactId} in sequence ${sequence_id}`);
      return { success: true, output: { ...inputData, sequence_enrolled_id: sequence_id } };
    } catch (error) {
      console.error(`[send_sequence] Error:`, error);
      return { success: false, output: inputData, error: error.message };
    }
  }
}

const automationEngine = new AutomationEngine();

export default automationEngine;
