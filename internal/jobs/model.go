package jobs

type CreateJobRequest struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type CreateJobResponse struct {
	ID      string `json:"id"`
	Message string `json:"message"`
	Type    string `json:"type"`
	Status  string `json:"status"`
}

type Job struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Payload   string `json:"payload"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}
