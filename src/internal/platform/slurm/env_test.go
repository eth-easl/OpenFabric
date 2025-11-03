package slurm

import (
	"os"
	"testing"
)

func TestIsSlurm(t *testing.T) {
	tests := []struct {
		name     string
		setup    func()
		expected bool
	}{
		{
			name: "SLURM environment variable set",
			setup: func() {
				os.Setenv("SLURM_JOB_ID", "12345")
			},
			expected: true,
		},
		{
			name: "SLURM environment variable not set",
			setup: func() {
				os.Unsetenv("SLURM_JOB_ID")
			},
			expected: false,
		},
		{
			name: "SLURM environment variable empty",
			setup: func() {
				os.Setenv("SLURM_JOB_ID", "")
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original value
			originalValue := os.Getenv("SLURM_JOB_ID")
			defer os.Setenv("SLURM_JOB_ID", originalValue)

			tt.setup()
			result := IsSlurm()
			if result != tt.expected {
				t.Errorf("IsSlurm() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetJobId(t *testing.T) {
	tests := []struct {
		name     string
		setup    func()
		expected string
	}{
		{
			name: "SLURM_JOB_ID set",
			setup: func() {
				os.Setenv("SLURM_JOB_ID", "12345")
			},
			expected: "12345",
		},
		{
			name: "SLURM_JOB_ID not set",
			setup: func() {
				os.Unsetenv("SLURM_JOB_ID")
			},
			expected: "",
		},
		{
			name: "SLURM_JOB_ID empty",
			setup: func() {
				os.Setenv("SLURM_JOB_ID", "")
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original value
			originalValue := os.Getenv("SLURM_JOB_ID")
			defer os.Setenv("SLURM_JOB_ID", originalValue)

			tt.setup()
			result := getJobId()
			if result != tt.expected {
				t.Errorf("getJobId() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetNodeId(t *testing.T) {
	tests := []struct {
		name     string
		setup    func()
		expected string
	}{
		{
			name: "SLURM_NODEID set",
			setup: func() {
				os.Setenv("SLURM_NODEID", "0")
			},
			expected: "0",
		},
		{
			name: "SLURM_NODEID not set",
			setup: func() {
				os.Unsetenv("SLURM_NODEID")
			},
			expected: "",
		},
		{
			name: "SLURM_NODEID empty",
			setup: func() {
				os.Setenv("SLURM_NODEID", "")
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original value
			originalValue := os.Getenv("SLURM_NODEID")
			defer os.Setenv("SLURM_NODEID", originalValue)

			tt.setup()
			result := getNodeId()
			if result != tt.expected {
				t.Errorf("getNodeId() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetRemainingTimeInSeconds(t *testing.T) {
	tests := []struct {
		name     string
		setup    func()
		expected int32
	}{
		{
			name: "SLURM_JOB_ID not set",
			setup: func() {
				os.Unsetenv("SLURM_JOB_ID")
			},
			expected: -1,
		},
		{
			name: "SLURM_JOB_ID empty",
			setup: func() {
				os.Setenv("SLURM_JOB_ID", "")
			},
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original value
			originalValue := os.Getenv("SLURM_JOB_ID")
			defer os.Setenv("SLURM_JOB_ID", originalValue)

			tt.setup()
			result := getRemainingTimeInSeconds()
			if result != tt.expected {
				t.Errorf("getRemainingTimeInSeconds() = %v, want %v", result, tt.expected)
			}
		})
	}

	// Skip the actual squeue test if not in SLURM environment
	if !IsSlurm() {
		t.Skip("Not in SLURM environment, skipping squeue test")
	}

	// Test actual squeue command if in SLURM environment
	t.Run("actual squeue command", func(t *testing.T) {
		result := getRemainingTimeInSeconds()
		// The result should be >= 0 if the command succeeds, or -1 if it fails
		if result < -1 {
			t.Errorf("getRemainingTimeInSeconds() returned unexpected value: %v", result)
		}
	})
}

func TestGetJobInfo(t *testing.T) {
	tests := []struct {
		name              string
		setup             func()
		expectedKeys      []string
		expectedJobID     string
		expectedNodeID    string
		expectedTimeValue string
	}{
		{
			name: "full SLURM environment",
			setup: func() {
				os.Setenv("SLURM_JOB_ID", "12345")
				os.Setenv("SLURM_NODEID", "0")
			},
			expectedKeys:      []string{"job_id", "node_id", "remaining_time"},
			expectedJobID:     "12345",
			expectedNodeID:    "0",
			expectedTimeValue: "-1", // When squeue is not available
		},
		{
			name: "only SLURM_JOB_ID set",
			setup: func() {
				os.Setenv("SLURM_JOB_ID", "67890")
				os.Unsetenv("SLURM_NODEID")
			},
			expectedKeys:      []string{"job_id", "node_id", "remaining_time"},
			expectedJobID:     "67890",
			expectedNodeID:    "",
			expectedTimeValue: "-1",
		},
		{
			name: "no SLURM environment",
			setup: func() {
				os.Unsetenv("SLURM_JOB_ID")
				os.Unsetenv("SLURM_NODEID")
			},
			expectedKeys:      []string{"job_id", "node_id", "remaining_time"},
			expectedJobID:     "",
			expectedNodeID:    "",
			expectedTimeValue: "-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original values
			originalJobID := os.Getenv("SLURM_JOB_ID")
			originalNodeID := os.Getenv("SLURM_NODEID")
			defer func() {
				os.Setenv("SLURM_JOB_ID", originalJobID)
				os.Setenv("SLURM_NODEID", originalNodeID)
			}()

			tt.setup()
			result := GetJobInfo()

			// Check that all expected keys are present
			for _, key := range tt.expectedKeys {
				if _, exists := result[key]; !exists {
					t.Errorf("Expected key '%s' not found in result", key)
				}
			}

			// Check specific values
			if result["job_id"] != tt.expectedJobID {
				t.Errorf("job_id = %v, want %v", result["job_id"], tt.expectedJobID)
			}
			if result["node_id"] != tt.expectedNodeID {
				t.Errorf("node_id = %v, want %v", result["node_id"], tt.expectedNodeID)
			}
			if result["remaining_time"] != tt.expectedTimeValue {
				t.Errorf("remaining_time = %v, want %v", result["remaining_time"], tt.expectedTimeValue)
			}
		})
	}
}