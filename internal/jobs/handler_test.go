package jobs

import "testing"

func TestValidateCreateRequest(t *testing.T) {
	tests := []struct {
		name    string
		request CreateJobRequest
		valid   bool
	}{
		{
			name: "valid request",
			request: CreateJobRequest{
				Type:    "email",
				Payload: "send welcome email",
			},
			valid: true,
		},
		{
			name: "missing type",
			request: CreateJobRequest{
				Payload: "send welcome email",
			},
			valid: false,
		},
		{
			name: "missing payload",
			request: CreateJobRequest{
				Type: "email",
			},
			valid: false,
		},
		{
			name:    "empty request",
			request: CreateJobRequest{},
			valid:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCreateRequest(test.request)

			if test.valid && err != nil {
				t.Fatalf("expected valid request, got error: %v", err)
			}

			if !test.valid && err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}
