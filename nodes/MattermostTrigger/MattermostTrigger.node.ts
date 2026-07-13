/*import {
	IExecuteFunctions,
} from 'n8n-core';*/

import {
	ApplicationError,
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
	NodeConnectionType,
} from 'n8n-workflow';
import {
	//MattermostAuthType,
	MattermostCredentialData,
} from '../../credentials/MattermostTriggerApi.credentials';

import { LoggerProxy as Logger } from 'n8n-workflow';

import {
	getAllowedEvents,
	getEventsByResource,
	InitClient,
} from './GenericFunctions';
import { Data, WebSocket } from 'ws';
import { MattermostTriggerOptions } from './MattermostTriggerDescription';

export class MattermostTrigger implements INodeType {
	description: INodeTypeDescription = {
		// Basic node details will go here
		properties: [
			// Resources and operations will go here
			...MattermostTriggerOptions,
		],
		displayName: 'Mattermost Event Trigger',
		name: 'mattermostTrigger',
		icon: 'file:mattermost-logo.svg',
		group: ['trigger'],
		version: 1,
		description: 'Receive Mattermost Events',
		defaults: {
			name: 'Mattermost Trigger',
		},
		inputs: [],
		outputs: ['main' as NodeConnectionType],
		credentials: [
			{
				name: 'mattermostTriggerApi',
				required: true,
			},
		],
	};

	methods = {
		loadOptions: {
			getEvents: getEventsByResource,
		},
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		let isClosing = true;
		let isAlive = false;
		let client: WebSocket;

		let pingInterval: NodeJS.Timeout;
		const wait = 10000; // 10 seconds

		const credentials = (await this.getCredentials(
			'mattermostTriggerApi'
		)) as MattermostCredentialData;
		const events = getAllowedEvents(this);

		const startConsumer = async () => {
			clearInterval(pingInterval);

			isClosing = false;
			// Establish the Websocket connection and set up event listeners

			//Create client
			Logger.info(
				`[${this.getNode.name}] Connecting to Mattermost WebSocket at "${credentials.baseUrl}"`,
				{
					workflowName: this.getWorkflow().name,
					workflowId: this.getWorkflow().id,
				}
			);
			if (client) {
				client.removeAllListeners();
				try {
					client.terminate();
				} catch (e) {}
			}
			client = InitClient(credentials.baseUrl, credentials.token || '');

			client.on('open', () => {
				isAlive = true;
				Logger.info(
					`[${this.getNode.name}] WebSocket connected successfully`,
					{
						workflowName: this.getWorkflow().name,
						workflowId: this.getWorkflow().id,
					}
				);

				pingInterval = setInterval(() => {
					if (!isAlive) {
						Logger.error(
							`[${this.getNode.name}] Server failed to respond to ping in time. Terminating...`,
							{
								workflowName: this.getWorkflow().name,
								workflowId: this.getWorkflow().id,
							}
						);
						return client.terminate();
					}

					isAlive = false;
					if (client.readyState === WebSocket.OPEN) {
						client.ping();
					}
				}, wait);
			});

			//Subscribe
			client.on('message', (data: Data) => {
				try {
					const messageObj = JSON.parse(data.toString());
					const event = messageObj.event;
					if (events.includes(event)) {
						this.emit([this.helpers.returnJsonArray([messageObj])]);
					} else {
						//console.log(
						//`Skipped event=${event}; allowedevents=${events};`
						//);
					}
				} catch (e) {
					Logger.error(
						`[${this.getNode.name}] Failed to parse WebSocket data: ${data}`,
						{
							workflowName: this.getWorkflow().name,
							workflowId: this.getWorkflow().id,
						}
					);
					throw new ApplicationError(
						`Failed to parse WebSocket data: ${data}`
					);
				}
			});

			client.on('pong', () => {
				isAlive = true;
			});

			client.on('error', (error) => {
				//throw new ApplicationError(`WebSocket error: ${error}`);
				Logger.error(
					`[${this.getNode.name}] WebSocket error encountered: ${error.message || error}`,
					{
						workflowName: this.getWorkflow().name,
						workflowId: this.getWorkflow().id,
					}
				);
			});

			client.on('close', (code, reason) => {
				clearInterval(pingInterval);
				Logger.info(
					`[${this.getNode.name}] WebSocket closed: ${code} - ${reason}`,
					{
						workflowName: this.getWorkflow().name,
						workflowId: this.getWorkflow().id,
					}
				);
				// Attempt to reconnect after a delay if the connection was not intentionally closed
				if (!isClosing) {
					setTimeout(() => {
						startConsumer();
					}, 500);
				}
			});
		};
		await startConsumer();

		const closeFunction = async () => {
			// Clean up any resources that were allocated during the connection
			isClosing = true;
			client.close();
		};

		const manualTriggerFunction = async () => {
			// Trigger the node manually
			await startConsumer();
		};

		return {
			closeFunction: closeFunction,
			manualTriggerFunction: manualTriggerFunction,
		};
	}
}
