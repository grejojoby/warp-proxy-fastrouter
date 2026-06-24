package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func handleChat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	// Parse Warp's payload
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err == nil {
		// Strip out the stream_options that break FastRouter's SSE
		delete(payload, "stream_options")
	}

	newBody, _ := json.Marshal(payload)

	// Forward to FastRouter
	req, _ := http.NewRequest("POST", "https://api.fastrouter.ai/api/v1/chat/completions", bytes.NewBuffer(newBody))
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	// Copy SSE headers and pipe the stream back to Warp
	for k, v := range resp.Header {
		w.Header().Set(k, v[0])
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func main() {
	http.HandleFunc("/v1/chat/completions", handleChat)
	log.Println("FastRouter Proxy running on http://localhost:8080/v1")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
