package s3

import (
	"bytes"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
)

func setupMinioContainer(t *testing.T) (testcontainers.Container, string, error) {
	ctx := t.Context()

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ACCESS_KEY": "minioadmin",
			"MINIO_SECRET_KEY": "minioadmin",
		},
		Cmd: []string{"server", "/data"},
		WaitingFor: wait.ForAll(
			wait.ForLog("API"),
			wait.ForListeningPort("9000/tcp"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return container, "", err
	}

	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return container, "", err
	}

	return container, host + ":" + mappedPort.Port(), nil
}

func TestS3Integration(t *testing.T) {
	ctx := t.Context()

	container, endpoint, err := setupMinioContainer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer container.Terminate(ctx)

	s3Storage := &S3{
		Host:      endpoint,
		Bucket:    "test-bucket",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		Prefix:    "test",
	}

	s3Storage.Logger = zap.NewNop()
	s3Storage.iowrap = &CleartextIO{}

	// Set up the client with HTTP instead of HTTPS for testing
	client, err := minio.New(s3Storage.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(s3Storage.AccessKey, s3Storage.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	s3Storage.Client = client

	// Create bucket
	err = s3Storage.Client.MakeBucket(ctx, s3Storage.Bucket, minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Test Store and Load
	testKey := "test-key"
	testData := []byte("test-data")

	err = s3Storage.Store(ctx, testKey, testData)
	if err != nil {
		t.Fatal(err)
	}

	// Test Exists
	if !s3Storage.Exists(ctx, testKey) {
		t.Error("Expected key to exist")
	}

	// Test Load
	loadedData, err := s3Storage.Load(ctx, testKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(loadedData) != string(testData) {
		t.Errorf("Expected %s, got %s", string(testData), string(loadedData))
	}

	// Test Lock
	err = s3Storage.Lock(ctx, testKey)
	if err != nil {
		t.Fatal(err)
	}

	// Test Unlock
	err = s3Storage.Unlock(ctx, testKey)
	if err != nil {
		t.Fatal(err)
	}

	// Test Delete
	err = s3Storage.Delete(ctx, testKey)
	if err != nil {
		t.Fatal(err)
	}

	if s3Storage.Exists(ctx, testKey) {
		t.Error("Expected key to not exist after deletion")
	}
}

func TestS3LockEdgeCases(t *testing.T) {
	ctx := t.Context()

	container, endpoint, err := setupMinioContainer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer container.Terminate(ctx)

	s3Storage := &S3{
		Host:      endpoint,
		Bucket:    "test-bucket",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		Prefix:    "test",
	}

	s3Storage.Logger = zap.NewNop()
	s3Storage.iowrap = &CleartextIO{}

	client, err := minio.New(s3Storage.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(s3Storage.AccessKey, s3Storage.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	s3Storage.Client = client

	err = s3Storage.Client.MakeBucket(ctx, s3Storage.Bucket, minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	testKey := "test-key-lock"
	err = s3Storage.Lock(ctx, testKey)
	if err != nil {
		t.Fatal(err)
	}

	err = s3Storage.Lock(ctx, testKey)
	if err == nil {
		t.Error("Expected error when trying to create lock that already exists")
	}

	invalidLockKey := "invalid-lock-key"
	invalidData := []byte("invalid lock content")

	lockPath := s3Storage.Prefix + "/" + invalidLockKey + ".lock"
	_, err = s3Storage.Client.PutObject(ctx, s3Storage.Bucket, lockPath, bytes.NewReader(invalidData), int64(len(invalidData)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	err = s3Storage.Unlock(ctx, invalidLockKey)
	if err == nil {
		t.Error("Expected error when trying to unlock file with invalid lock content")
	}

	timeoutKey := "timeout-key"
	err = s3Storage.Lock(ctx, timeoutKey)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	exists := s3Storage.Exists(ctx, timeoutKey+".lock")
	if !exists {
		t.Error("Lock file should still exist before timeout")
	}

	unlockKey := "unlock-test-key"
	err = s3Storage.Lock(ctx, unlockKey)
	if err != nil {
		t.Fatal(err)
	}

	err = s3Storage.Unlock(ctx, unlockKey)
	if err != nil {
		t.Fatal(err)
	}

	exists = s3Storage.Exists(ctx, unlockKey+".lock")
	if exists {
		t.Error("Lock file should not exist after unlock")
	}

	err = s3Storage.Unlock(ctx, "non-existent-key")
	if err == nil {
		t.Error("Expected error when trying to unlock non-existent lock")
	}
}
