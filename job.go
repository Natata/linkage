package linkage

import "linkage/proto/job"

// Job struct
// code is for dispatcher know what kind of worker response for this job
type Job struct {
	Payload  string            `json:"payload"`
	Metadata map[string]string `json:"metadata"`
}

// CreateJob creates a job and the created time
func CreateJob(payload string, metadata map[string]string) *Job {
	return &Job{
		Payload:  payload,
		Metadata: metadata,
	}
}

// GetPayload is nil-safe method to get payload in job
func (j *Job) GetPayload() string {
	if j == nil {
		return ""
	}

	return j.Payload
}

// GetMetadata is nil-safe method to get metadata in job
func (j *Job) GetMetadata() map[string]string {
	if j == nil {
		return map[string]string{}
	}

	return j.Metadata
}

func toGRPCJob(j *Job) *job.Job {
	return &job.Job{
		Payload:  j.Payload,
		Metadata: j.Metadata,
	}
}

func toLinkageJob(j *job.Job) *Job {
	return &Job{
		Payload:  j.GetPayload(),
		Metadata: j.GetMetadata(),
	}
}
