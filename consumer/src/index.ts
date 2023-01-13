import type { MessageBody, ActivityMessageBody } from 'wildebeest/backend/src/types/queue'
import type { JWK } from 'wildebeest/backend/src/webpush/jwk'
import type { Actor } from 'wildebeest/backend/src/activitypub/actors'
import * as actors from 'wildebeest/backend/src/activitypub/actors'
import * as timeline from 'wildebeest/backend/src/mastodon/timeline'
import * as notification from 'wildebeest/backend/src/mastodon/notification'
import * as activityHandler from 'wildebeest/backend/src/activitypub/activities/handle'
import type { Activity } from 'wildebeest/backend/src/activitypub/activities'
import { MessageType } from 'wildebeest/backend/src/types/queue'

type Env = {
	DATABASE: D1Database
	DOMAIN: string
	ADMIN_EMAIL: string
	KV_CACHE: KVNamespace
}

export default {
	async queue(batch: MessageBatch<MessageBody>, env: Env, ctx: ExecutionContext) {
		for (const message of batch.messages) {
			try {
				const actor = await actors.getPersonById(env.DATABASE, new URL(message.body.actorId))
				if (actor === null) {
					console.warn(`actor ${message.body.actorId} is missing`)
					return
				}

				switch (message.body.type) {
					case MessageType.Inbox: {
						await handleActivityMessage(env, actor, message.body as ActivityMessageBody)
						break
					}
					default:
						throw new Error('unsupported message type: ' + message.body.type)
				}
			} catch (err: any) {
				console.error(err.stack)
				// TODO: add sentry
			}
		}
	},
}

async function handleActivityMessage(env: Env, actor: Actor, message: ActivityMessageBody) {
	const domain = env.DOMAIN
	const db = env.DATABASE
	const adminEmail = env.ADMIN_EMAIL
	const cache = env.KV_CACHE
	const activity = message.activity

	await activityHandler.handle(domain, activity, db, message.userKEK, adminEmail, message.vapidKeys)

	// Assuming we received new posts or a like, pregenerate the user's timelines
	// and notifications.
	await Promise.all([
		timeline.pregenerateTimelines(domain, db, cache, actor),
		notification.pregenerateNotifications(db, cache, actor),
	])
}
