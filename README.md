# Portcullis
### The iron gate between your bot's business logic and the Discord gateway.

Portcullis is a proxy that holds your bot's Discord gateway connections and republishes every event it receives onto 
whatever bridge you configure.

## Why did I build it?

When building Novus, I wanted to deploy frequently as some days I was pushing multiple changes to what would end up being
in the bot's codebase. However, I had to stop myself because every single deploy meant restarting the connections. Sure,
resumes exist but the identify budget isn't infinite and the bigger the bot the longer the startup times.

Portcullis' goal is to maintain that connection in perpetuity which allows you to decouple the bot's business logic from
the gateway connection entirely enabling cheap restarts and scaling based on the events you're processing.

Overall Portcullis gets you:
- Decoupling of the business logic from the gateway sessions
- Filtering controls so you only ever consume what you want to consume
- Horizontal scaling without worrying about websockets
- Multiple bridges which allows you to handle the same event in multiple ways
- No payload modification, what Discord sends is what is published
- Built in OTel support and Sentry-compatible provider support
- HTTP server to get health of system and shards as well as send commands to the gateway

## Understanding the stream

- **Subject**: `<subject_prefix>.<EVENT_TYPE>`, e.g. `events.MESSAGE_CREATE`. Subscribe with a filter per event type, 
  or `events.>` for everything you publish.
- **Retention**: the stream uses **work-queue retention**; each message is delivered to one consumer and removed once 
  acked. 
- **Consuming**: The intended shape is a single durable consumer that handles individual events or the entire stream. If 
  you need multiple independent consumers for the same event then you should structure the single consumer to fan out.


## Running in production

- **Signals**: `SIGINT` / `SIGTERM` trigger a graceful shutdown which stops all shards, queued frames drain to JetStream, 
  pending async publishes complete, and the NATS connection drains before exit.
- **Fatal exits are intentional and used with caution**: an identify budget too low at startup, a shard that ends 
  permanently (fatal close code, flapping, dial retries exhausted), or an unexpected NATS disconnect all terminate the
  process so your restart policy reacts accordingly.
- **Restarts are cheap when possible**: with unchanged intents and shard count, shards RESUME from stored sessions unless
  told otherwise by Discord.
- **NATS is a hard dependency**: the process exits if the connection closes unexpectedly. Reconnects (up to 60, 2s apart) 
  are handled by the NATS client before that point.
- **Limits worth knowing**: websocket frames up to 128 MB are accepted; shard shutdown waits up to 15s per shutdown cycle.

## Bridges
### Webhook
When using the webhook bridge, Portcullis expects either a 2xx or 429 when "fire and forget" is disabled. 

When a 2xx status code is received Portcullis assumes a successful call. It does not check the body of the response.

429 looks for `Retry-After` header defaulting to zero. `Retry-After` can be HTTP time handled by 
[`http.ParseTime`](https://pkg.go.dev/net/http#ParseTime) or seconds to wait. After configured `max_retries` it will
drop and log. You should use 429 for whenever an error occurs and you want Portcullis to send it again.

Any other status code is simply logged with a snippet of the body provided.

## AI Disclosure

AI tools are used as part of the feedback loop and portions of the development process. Despite AI usage, all code is
reviewed entirely by human to ensure nothing is lost or added without need.

Some example uses:
- Claude was used to review initial stages to assist in checking for invalid/incorrect
  information in the design(s) or find potential gaps in proposed approaches.
- Claude Fable was used at checkpoints for overall auditing purposes and generating reports focusing on security, gaps,
  and potential chokepoints.
- OpenCode was used for a refactor of a new JSONText implementation to clean up prototyping logic, simplify complex
  functions, break apart files, etc.

## License

[AGPL-3.0](LICENSE)
