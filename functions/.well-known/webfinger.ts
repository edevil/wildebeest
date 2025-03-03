// https://www.rfc-editor.org/rfc/rfc7033

import { parseHandle } from '../../backend/src/utils/parse'
import { getPersonById, actorURL } from 'wildebeest/backend/src/activitypub/actors'
import type { Env } from '../../backend/src/types/env'
import type { WebFingerResponse } from '../../backend/src/webfinger'

export const onRequest: PagesFunction<Env, any> = async ({ request, env }) => {
	return handleRequest(request, env.DATABASE)
}

const headers = {
	'content-type': 'application/jrd+json',
	'cache-control': 'max-age=3600, public',
}

export async function handleRequest(request: Request, db: D1Database): Promise<Response> {
	const url = new URL(request.url)
	const domain = url.hostname
	const resource = url.searchParams.get('resource')
	if (!resource) {
		return new Response('', { status: 400 })
	}

	const parts = resource.split(':')
	if (parts.length !== 2 || parts[0] !== 'acct') {
		return new Response('', { status: 400 })
	}

	const handle = parseHandle(parts[1])
	if (handle.domain === null) {
		return new Response('', { status: 400 })
	}

	if (handle.domain !== domain) {
		return new Response('', { status: 403 })
	}

	const person = await getPersonById(db, actorURL(domain, handle.localPart))
	if (person === null) {
		return new Response('', { status: 404 })
	}

	const jsonLink = person.id.toString()

	const res: WebFingerResponse = {
		subject: `acct:${handle.localPart}@${handle.domain}`,
		aliases: [jsonLink],
		links: [
			{
				rel: 'self',
				type: 'application/activity+json',
				href: jsonLink,
			},
		],
	}

	return new Response(JSON.stringify(res), { status: 200, headers })
}
