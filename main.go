package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const upstreamURL = "https://api.fastrouter.ai/api/v1/chat/completions"

var hopByHopHeaders = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailers":            true,
	"transfer-encoding":   true,
	"upgrade":             true,
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", handleChat)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Println("FastRouter proxy listening on :8080")
	log.Fatal(srv.ListenAndServe())
}

func handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		writeCORS(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		log.Printf("error: method not allowed: %s %s", r.Method, r.URL.Path)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("error: read request body: %v", err)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	newBody, bodyErr := prepareRequestBody(body)
	if bodyErr != nil {
		log.Printf("warn: request body not modified: %v", bodyErr)
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(newBody))
	if err != nil {
		log.Printf("error: build upstream request: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		log.Printf("error: upstream request failed: %v", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		log.Printf("error: upstream status=%d content-type=%q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	copyResponseHeaders(w, resp)
	writeCORS(w)
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(resp.StatusCode)

	if !isEventStream(resp) {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error: read upstream response body: %v", err)
			return
		}
		if resp.StatusCode >= 400 {
			log.Printf("error: upstream response body: %s", truncateForLog(string(respBody), 500))
		}
		if _, err := w.Write(respBody); err != nil {
			log.Printf("error: write response body: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	relaySSE(w, resp.Body)
}

func prepareRequestBody(body []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body, err
	}

	// Warp sends this; FastRouter's SSE output does not match Warp when it is set.
	delete(payload, "stream_options")

	marshaled, err := json.Marshal(payload)
	if err != nil {
		return body, err
	}
	return marshaled, nil
}

func isEventStream(resp *http.Response) bool {
	return strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
}

func copyResponseHeaders(w http.ResponseWriter, resp *http.Response) {
	for k, vals := range resp.Header {
		if hopByHopHeaders[strings.ToLower(k)] {
			continue
		}
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
}

func writeCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func relaySSE(w http.ResponseWriter, body io.Reader) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("warn: response writer does not support flush; streaming passthrough")
		if _, err := io.Copy(w, body); err != nil {
			log.Printf("error: stream passthrough failed: %v", err)
		}
		return
	}

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	write := func(chunk string) {
		if _, err := w.Write([]byte(chunk)); err != nil {
			log.Printf("error: write stream chunk: %v", err)
			return
		}
		flusher.Flush()
	}

	writeEvent := func(data string) {
		write("data: " + data + "\n\n")
	}

	skipNextEmpty := false
	sawDone := false
	eventsSent := 0

	for scanner.Scan() {
		line := scanner.Text()

		if skipNextEmpty {
			skipNextEmpty = false
			if line == "" {
				continue
			}
		}

		// SSE comments (": ...") crash Warp's openai-go ssestream parser.
		if strings.HasPrefix(line, ":") {
			continue
		}

		if line == "" {
			continue
		}

		if !strings.HasPrefix(line, "data: ") {
			log.Printf("warn: skipping non-data SSE line: %s", truncateForLog(line, 200))
			continue
		}

		data := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		if data == "" {
			log.Printf("warn: skipping empty SSE data event")
			continue
		}

		if data == "[DONE]" {
			sawDone = true
			writeEvent("[DONE]")
			continue
		}

		if !json.Valid([]byte(data)) {
			log.Printf("warn: skipping invalid JSON chunk: %s", truncateForLog(data, 200))
			continue
		}

		cleaned, keep := sanitizeChunk(data)
		if !keep {
			skipNextEmpty = true
			continue
		}

		writeEvent(cleaned)
		skipNextEmpty = true
		eventsSent++
	}

	if err := scanner.Err(); err != nil {
		log.Printf("error: upstream stream read failed after %d events: %v", eventsSent, err)
	}

	if !sawDone {
		log.Printf("warn: upstream stream ended without [DONE] after %d events; appending terminator", eventsSent)
		writeEvent("[DONE]")
	}
}

func sanitizeChunk(data string) (string, bool) {
	var chunk map[string]any
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return data, true
	}

	choices, ok := chunk["choices"].([]any)
	if !ok || len(choices) == 0 {
		return data, true
	}

	choice, ok := choices[0].(map[string]any)
	if !ok {
		return data, true
	}

	if finishReason, ok := choice["finish_reason"].(string); ok && finishReason != "" {
		return data, true
	}

	delta, ok := choice["delta"].(map[string]any)
	if !ok {
		return data, true
	}

	delete(delta, "reasoning_content")
	delete(delta, "reasoning")

	if len(delta) == 0 {
		return "", false
	}

	if content, ok := delta["content"].(string); ok && content == "" && len(delta) == 1 {
		return "", false
	}

	choice["delta"] = delta
	choices[0] = choice
	chunk["choices"] = choices

	out, err := json.Marshal(chunk)
	if err != nil {
		log.Printf("warn: sanitize chunk marshal failed: %v data=%s", err, truncateForLog(data, 200))
		return data, true
	}
	return string(out), true
}

func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
