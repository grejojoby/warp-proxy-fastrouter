const UPSTREAM_URL = "https://api.fastrouter.ai/api/v1/chat/completions";

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

const STREAM_HEADERS: Record<string, string> = {
  ...CORS_HEADERS,
  "Content-Type": "text/event-stream; charset=utf-8",
  "Cache-Control": "no-cache",
  "X-Accel-Buffering": "no",
};

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/healthz") {
      return new Response("ok", { status: 200 });
    }

    if (url.pathname !== "/v1/chat/completions") {
      return jsonError("not found", 404);
    }

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (request.method !== "POST") {
      console.error(`error: method not allowed: ${request.method} ${url.pathname}`);
      return jsonError("method not allowed", 405);
    }

    let bodyText: string;
    try {
      bodyText = await request.text();
    } catch (error) {
      console.error("error: read request body:", error);
      return jsonError("failed to read body", 400);
    }

    const prepared = prepareRequestBody(bodyText);
    if (prepared.warning) {
      console.warn("warn: request body not modified:", prepared.warning);
    }

    let upstream: Response;
    try {
      upstream = await fetch(UPSTREAM_URL, {
        method: "POST",
        headers: {
          Authorization: request.headers.get("Authorization") ?? "",
          "Content-Type": "application/json",
          Accept: "text/event-stream",
        },
        body: prepared.body,
      });
    } catch (error) {
      console.error("error: upstream request failed:", error);
      return jsonError(String(error), 502);
    }

    if (upstream.status >= 400) {
      console.error(
        `error: upstream status=${upstream.status} content-type=${JSON.stringify(upstream.headers.get("content-type"))}`,
      );
    }

    const contentType = upstream.headers.get("content-type") ?? "";
    if (!contentType.includes("text/event-stream")) {
      const responseBody = await upstream.text();
      if (upstream.status >= 400) {
        console.error("error: upstream response body:", truncateForLog(responseBody, 500));
      }
      return new Response(responseBody, {
        status: upstream.status,
        headers: {
          ...CORS_HEADERS,
          "Content-Type": contentType || "application/json",
          "Cache-Control": "no-cache",
        },
      });
    }

    if (!upstream.body) {
      console.error("error: upstream stream body missing");
      return jsonError("upstream stream body missing", 502);
    }

    return new Response(relaySSE(upstream.body), {
      status: upstream.status,
      headers: STREAM_HEADERS,
    });
  },
};

function jsonError(message: string, status: number): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "application/json",
    },
  });
}

function prepareRequestBody(body: string): { body: string; warning?: string } {
  try {
    const payload = JSON.parse(body) as Record<string, unknown>;
    delete payload.stream_options;
    return { body: JSON.stringify(payload) };
  } catch (error) {
    return { body, warning: String(error) };
  }
}

function relaySSE(upstreamBody: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();

  let buffer = "";
  let skipNextEmpty = false;
  let sawDone = false;
  let eventsSent = 0;

  const reader = upstreamBody.getReader();

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      const writeEvent = (data: string) => {
        controller.enqueue(encoder.encode(`data: ${data}\n\n`));
      };

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }

          buffer += decoder.decode(value, { stream: true });

          let newlineIndex = buffer.indexOf("\n");
          while (newlineIndex !== -1) {
            const line = buffer.slice(0, newlineIndex);
            buffer = buffer.slice(newlineIndex + 1);

            const result = processSSELine(line, {
              skipNextEmpty,
              sawDone,
              eventsSent,
              writeEvent,
            });

            skipNextEmpty = result.skipNextEmpty;
            sawDone = result.sawDone;
            eventsSent = result.eventsSent;

            newlineIndex = buffer.indexOf("\n");
          }
        }

        if (buffer.length > 0) {
          const result = processSSELine(buffer, {
            skipNextEmpty,
            sawDone,
            eventsSent,
            writeEvent,
          });
          skipNextEmpty = result.skipNextEmpty;
          sawDone = result.sawDone;
          eventsSent = result.eventsSent;
        }
      } catch (error) {
        console.error(`error: upstream stream read failed after ${eventsSent} events:`, error);
      } finally {
        if (!sawDone) {
          console.warn(
            `warn: upstream stream ended without [DONE] after ${eventsSent} events; appending terminator`,
          );
          writeEvent("[DONE]");
        }
        controller.close();
      }
    },
  });
}

type SSEState = {
  skipNextEmpty: boolean;
  sawDone: boolean;
  eventsSent: number;
  writeEvent: (data: string) => void;
};

function processSSELine(rawLine: string, state: SSEState): SSEState {
  let { skipNextEmpty, sawDone, eventsSent, writeEvent } = state;
  const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;

  if (skipNextEmpty) {
    skipNextEmpty = false;
    if (line === "") {
      return { skipNextEmpty, sawDone, eventsSent, writeEvent };
    }
  }

  if (line.startsWith(":")) {
    return { skipNextEmpty, sawDone, eventsSent, writeEvent };
  }

  if (line === "") {
    return { skipNextEmpty, sawDone, eventsSent, writeEvent };
  }

  if (!line.startsWith("data: ")) {
    console.warn("warn: skipping non-data SSE line:", truncateForLog(line, 200));
    return { skipNextEmpty, sawDone, eventsSent, writeEvent };
  }

  const data = line.slice("data: ".length).trim();
  if (data === "") {
    console.warn("warn: skipping empty SSE data event");
    return { skipNextEmpty, sawDone, eventsSent, writeEvent };
  }

  if (data === "[DONE]") {
    sawDone = true;
    writeEvent("[DONE]");
    return { skipNextEmpty, sawDone, eventsSent, writeEvent };
  }

  if (!isValidJSON(data)) {
    console.warn("warn: skipping invalid JSON chunk:", truncateForLog(data, 200));
    return { skipNextEmpty, sawDone, eventsSent, writeEvent };
  }

  const sanitized = sanitizeChunk(data);
  if (!sanitized.keep) {
    return { skipNextEmpty: true, sawDone, eventsSent, writeEvent };
  }

  writeEvent(sanitized.data);
  return { skipNextEmpty: true, sawDone, eventsSent: eventsSent + 1, writeEvent };
}

function isValidJSON(value: string): boolean {
  try {
    JSON.parse(value);
    return true;
  } catch {
    return false;
  }
}

function sanitizeChunk(data: string): { data: string; keep: boolean } {
  let chunk: Record<string, unknown>;
  try {
    chunk = JSON.parse(data) as Record<string, unknown>;
  } catch {
    return { data, keep: true };
  }

  const choices = chunk.choices;
  if (!Array.isArray(choices) || choices.length === 0) {
    return { data, keep: true };
  }

  const choice = choices[0];
  if (!isRecord(choice)) {
    return { data, keep: true };
  }

  if (typeof choice.finish_reason === "string" && choice.finish_reason !== "") {
    return { data, keep: true };
  }

  const delta = choice.delta;
  if (!isRecord(delta)) {
    return { data, keep: true };
  }

  delete delta.reasoning_content;
  delete delta.reasoning;

  const deltaKeys = Object.keys(delta);
  if (deltaKeys.length === 0) {
    return { data: "", keep: false };
  }

  if (deltaKeys.length === 1 && delta.content === "") {
    return { data: "", keep: false };
  }

  choice.delta = delta;
  choices[0] = choice;
  chunk.choices = choices;

  try {
    return { data: JSON.stringify(chunk), keep: true };
  } catch (error) {
    console.warn(
      "warn: sanitize chunk stringify failed:",
      error,
      "data=",
      truncateForLog(data, 200),
    );
    return { data, keep: true };
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function truncateForLog(value: string, max: number): string {
  if (value.length <= max) {
    return value;
  }
  return `${value.slice(0, max)}...`;
}
