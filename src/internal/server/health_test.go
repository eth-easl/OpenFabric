package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestHealthStatusCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create a new Gin router with the health endpoint
	router := gin.New()
	router.GET("/health", healthStatusCheck)

	// Create a test request
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create a response recorder to record the response
	w := httptest.NewRecorder()

	// Serve the request
	router.ServeHTTP(w, req)

	// Check the status code
	assert.Equal(t, http.StatusOK, w.Code)

	// Check the response body
	expectedBody := `{"status":"ok"}`
	assert.JSONEq(t, expectedBody, w.Body.String())
}

func TestHealthStatusCheckWithMethod(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Test different HTTP methods
	tests := []struct {
		method string
		status int
	}{
		{"GET", http.StatusOK},
		{"POST", http.StatusOK}, // health endpoint accepts any method due to gin routing
		{"PUT", http.StatusOK},
		{"PATCH", http.StatusOK},
		{"DELETE", http.StatusOK},
		{"OPTIONS", http.StatusOK},
		{"HEAD", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			router := gin.New()
			router.Any("/health", healthStatusCheck)

			req, err := http.NewRequest(tt.method, "/health", nil)
			if err != nil {
				t.Fatal(err)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.status, w.Code)

			assert.JSONEq(t, `{"status":"ok"}`, w.Body.String())
		})
	}
}

func TestHealthStatusCheckResponseHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.GET("/health", healthStatusCheck)

	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Check that Content-Type is set to application/json
	assert.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))
}