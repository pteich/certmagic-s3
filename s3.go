package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type S3 struct {
	Logger *zap.Logger

	// S3
	Client    *minio.Client
	Host      string `json:"host"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Prefix    string `json:"prefix"`

	// EncryptionKey is optional. If you do not wish to encrypt your certficates and key inside the S3 bucket, leave it empty.
	EncryptionKey string `json:"encryption_key"`

	iowrap IO
}

func init() {
	caddy.RegisterModule(new(S3))
}

func (s3 *S3) Provision(context caddy.Context) error {
	s3.Logger = context.Logger(s3)

	// S3 Client
	client, err := minio.New(s3.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(s3.AccessKey, s3.SecretKey, ""),
		Secure: true,
	})

	if err != nil {
		return err
	}

	s3.Client = client

	if len(s3.EncryptionKey) == 0 {
		s3.Logger.Info("Clear text certificate storage active")
		s3.iowrap = &CleartextIO{}
	} else if len(s3.EncryptionKey) != 32 {
		s3.Logger.Error("encryption key must have exactly 32 bytes")
		return errors.New("encryption key must have exactly 32 bytes")
	} else {
		s3.Logger.Info("Encrypted certificate storage active")
		sb := &SecretBoxIO{}
		copy(sb.SecretKey[:], []byte(s3.EncryptionKey))
		s3.iowrap = sb
	}

	return nil
}

func (s3 *S3) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.s3",
		New: func() caddy.Module {
			return new(S3)
		},
	}
}

var (
	LockExpiration   = 2 * time.Minute
	LockPollInterval = 1 * time.Second
	LockTimeout      = 15 * time.Second
)

func (s3 *S3) Lock(ctx context.Context, key string) error {
	s3.Logger.Info(fmt.Sprintf("Lock: %v", s3.objName(key)))
	var startedAt = time.Now()

	data, err := s3.getLockFile(ctx, key)
	if err == nil {
		lt, err := time.Parse(time.RFC3339, data)
		if err == nil && lt.Add(LockTimeout).After(time.Now()) {
			return fmt.Errorf("lock already exists and is still valid")
		}
	}

	for {
		err = s3.putLockFile(ctx, key)
		if err == nil {
			return nil
		}

		lt, err := time.Parse(time.RFC3339, data)
		if err != nil {
			return s3.putLockFile(ctx, key)
		}

		if lt.Add(LockTimeout).Before(time.Now()) {
			return s3.putLockFile(ctx, key)
		}

		if startedAt.Add(LockTimeout).Before(time.Now()) {
			return fmt.Errorf("timeout while acquiring lock")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(LockPollInterval):
			continue
		}
	}
}

func (s3 *S3) getLockFile(ctx context.Context, key string) (string, error) {
	obj, err := s3.Client.GetObject(ctx, s3.Bucket, s3.objLockName(key), minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}

	defer obj.Close()
	buf, err := io.ReadAll(obj)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (s3 *S3) putLockFile(ctx context.Context, key string) error {
	// Object does not exist, we're creating a lock file.
	r := bytes.NewReader([]byte(time.Now().Format(time.RFC3339)))
	_, err := s3.Client.PutObject(ctx, s3.Bucket, s3.objLockName(key), r, int64(r.Len()), minio.PutObjectOptions{})
	return err
}

func (s3 *S3) Unlock(ctx context.Context, key string) error {
	s3.Logger.Info(fmt.Sprintf("Release lock: %v", s3.objName(key)))

	// Prüfe ob die Lock-Datei existiert und gültig ist
	data, err := s3.getLockFile(ctx, key)
	if err != nil {
		return fmt.Errorf("lock file does not exist")
	}

	// Validiere den Lock-Datei-Inhalt
	_, err = time.Parse(time.RFC3339, data)
	if err != nil {
		return fmt.Errorf("invalid lock file content")
	}

	// Lösche die Lock-Datei
	return s3.Client.RemoveObject(ctx, s3.Bucket, s3.objLockName(key), minio.RemoveObjectOptions{})
}

func (s3 *S3) Store(ctx context.Context, key string, value []byte) error {
	r := s3.iowrap.ByteReader(value)
	s3.Logger.Info(fmt.Sprintf("Store: %v, %v bytes", s3.objName(key), len(value)))
	_, err := s3.Client.PutObject(ctx,
		s3.Bucket,
		s3.objName(key),
		r,
		r.Len(),
		minio.PutObjectOptions{},
	)
	return err
}

func (s3 *S3) Load(ctx context.Context, key string) ([]byte, error) {
	s3.Logger.Info(fmt.Sprintf("Load: %v", s3.objName(key)))
	r, err := s3.Client.GetObject(ctx, s3.Bucket, s3.objName(key), minio.GetObjectOptions{})
	if err != nil {
		if err.Error() == "The specified key does not exist." {
			return nil, fs.ErrNotExist
		}
		return nil, err
	} else if r != nil {
		// AWS (at least) doesn't return an error on key doesn't exist. We have
		// to examine the empty object returned.
		_, err = r.Stat()
		if err != nil {
			er := minio.ToErrorResponse(err)
			if er.StatusCode == 404 {
				return nil, fs.ErrNotExist
			}
		}
	}
	defer r.Close()
	buf, err := io.ReadAll(s3.iowrap.WrapReader(r))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (s3 *S3) Delete(ctx context.Context, key string) error {
	s3.Logger.Info(fmt.Sprintf("Delete: %v", s3.objName(key)))
	return s3.Client.RemoveObject(ctx, s3.Bucket, s3.objName(key), minio.RemoveObjectOptions{})
}

func (s3 *S3) Exists(ctx context.Context, key string) bool {
	s3.Logger.Info(fmt.Sprintf("Exists: %v", s3.objName(key)))
	_, err := s3.Client.StatObject(ctx, s3.Bucket, s3.objName(key), minio.StatObjectOptions{})
	return err == nil
}

func (s3 *S3) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	var keys []string
	for obj := range s3.Client.ListObjects(ctx, s3.Bucket, minio.ListObjectsOptions{
		Prefix:    s3.objName(""),
		Recursive: true,
	}) {
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (s3 *S3) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	s3.Logger.Info(fmt.Sprintf("Stat: %v", s3.objName(key)))
	var ki certmagic.KeyInfo
	oi, err := s3.Client.StatObject(ctx, s3.Bucket, s3.objName(key), minio.StatObjectOptions{})
	if err != nil {
		return ki, fs.ErrNotExist
	}
	ki.Key = key
	ki.Size = oi.Size
	ki.Modified = oi.LastModified
	ki.IsTerminal = true
	return ki, nil
}

func (s3 *S3) objName(key string) string {
	return fmt.Sprintf("%s/%s", strings.TrimPrefix(s3.Prefix, "/"), strings.TrimPrefix(key, "/"))
}

func (s3 *S3) objLockName(key string) string {
	return s3.objName(key) + ".lock"
}

// CertMagicStorage converts s to a certmagic.Storage instance.
func (s3 *S3) CertMagicStorage() (certmagic.Storage, error) {
	return s3, nil
}

func (s3 *S3) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		key := d.Val()
		var value string

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "host":
			s3.Host = value
		case "bucket":
			s3.Bucket = value
		case "access_key":
			s3.AccessKey = value
		case "secret_key":
			s3.SecretKey = value
		case "prefix":
			if value != "" {
				s3.Prefix = value
			} else {
				s3.Prefix = "acme"
			}
		case "encryption_key":
			s3.EncryptionKey = value
		}
	}
	return nil
}

var (
	_ caddy.Provisioner      = (*S3)(nil)
	_ caddy.StorageConverter = (*S3)(nil)
	_ caddyfile.Unmarshaler  = (*S3)(nil)
)
