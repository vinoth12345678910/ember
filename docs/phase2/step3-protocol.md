# Step 3 - The wire protocol module (ember-protocol)

## What we are building
Plain Java types and codecs for the three requests (PRODUCE, FETCH, METADATA) and their responses, as bytes in a `ByteBuffer`. No sockets, no Netty. The exact byte layouts live in `docs/PROTOCOL.md`.

## Why it is designed this way
**A protocol is a contract written in bytes.** With Express you send JSON and the framework parses it; if a field is missing you get `undefined`. On a raw TCP connection there is no JSON, no HTTP, not even message boundaries: TCP is just a stream of bytes. So we need (1) *framing* - an INT32 length in front of every message so the receiver knows where one ends - and (2) a fixed layout for the fields.

**Binary, not JSON.** Fetch responses are the raw bytes of the log file (zero-copy, step 6). If the response were JSON we would have to decode and re-encode every record. Binary also means a fixed, tiny overhead per field.

**Shared module.** Broker and client both depend on `ember-protocol`, so there is exactly one place that knows the layouts. It depends on `ember-storage` only for `LogRecord`: the fetch body is a sequence of `LogRecord` encodings.

**The lesson from the record format: never trust a length.** A hostile or buggy peer can send `recordCount = 2_000_000_000`. If we did `new ArrayList<>(count)` or `new byte[len]` we would crash with OutOfMemoryError. Every count/length is checked against the bytes actually *remaining* in the buffer before we allocate or read, and each element has a minimum size, so `count * minSize <= remaining`. Trailing bytes after a body are also an error (they mean the two sides disagree about the layout).

**Errors carry what the broker needs.** `ProtocolException` holds the `ErrorCode` to answer with (INVALID_REQUEST, UNSUPPORTED_API, UNSUPPORTED_VERSION) and the `correlationId` if the 8-byte header could be read. Header unreadable -> id is -1 -> the broker can only close the connection.

**Correlation id.** The client picks a number per request and the broker echoes it. It is how a client knows that response N belongs to request N (like a request id in HTTP/2 or JSON-RPC). With one request in flight it is also a sanity check that we are not out of sync.

**FetchResponse header is encoded separately.** The broker writes `[length][corr][err][topic][partition][logStart][hwm][recordsLength]` as a small buffer and then streams the records straight from the file. So `FetchResponse.encodeHeader` exists, and a test proves header + records bytes == whole-frame encode.

## Alternatives I rejected and why
* JSON/HTTP: easy to debug, but kills zero-copy and is slower.
* Protobuf/Avro: a library plus code generation; the point of Ember is to learn the bytes.
* Exceptions only (no error code in the exception): the broker would have to guess which error code to send.

## Tradeoffs and honest limits
* No compression, no checksums on the request itself (TCP has its own; records have CRCs).
* `apiVersion` must be 0; there is no negotiation yet. The header and the `apiVersion` field are the forward-compat hook.
* Topic names are validated in the codec, so a bad name is INVALID_REQUEST even for FETCH/METADATA.

## How it is tested
Round trip of every request and response; every malformed case (negative counts, lengths past the end, trailing bytes, bad UTF-8, topic too long/empty/bad characters, unsupported apiKey/version, short header); fetch header encoding equals whole-frame encoding; `RecordDecoder` with a partial trailing record.
