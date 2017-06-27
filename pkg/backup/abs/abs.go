// Copyright 2017 The etcd-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package abs

import (
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	v1 = "v1/"
)

// ABS is a helper to wrap complex ABS logic
type ABS struct {
	container *storage.Container
	prefix    string
	client    *storage.BlobStorageClient
}

// New returns a new ABS object for a given container using credentials set in the environment
func New(container, prefix string) (*ABS, error) {
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if accountName == "" {
		return nil, fmt.Errorf("missing required environment variable of AZURE_STORAGE_ACCOUNT")
	}
	accountKey := os.Getenv("AZURE_STORAGE_KEY")
	if accountKey == "" {
		return nil, fmt.Errorf("missing required environment variable of AZURE_STORAGE_KEY")
	}
	basicClient, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("Create ABS client failed: %v", err)
	}

	return NewFromClient(container, prefix, &basicClient)
}

// NewFromClient returns a new ABS object for a given container using the supplied storageClient
func NewFromClient(container, prefix string, storageClient *storage.Client) (*ABS, error) {
	client := storageClient.GetBlobService()

	return &ABS{
		container: client.GetContainerReference(container),
		prefix:    prefix,
		client:    &client,
	}, nil
}

// Put puts a chunk of data into a ABS container using the provided key for its reference
func (w *ABS) Put(key string, chunk []byte) error {
	blobName := path.Join(v1, w.prefix, key)
	blob := w.container.GetBlobReference(blobName)

	opts := &storage.PutBlobOptions{}
	err := blob.CreateBlockBlob(opts)
	if err != nil {
		return fmt.Errorf("create block blob failed: %v", err)
	}

	blockID := base64.StdEncoding.EncodeToString(randBytes(6))
	err = blob.PutBlock(blockID, chunk, nil)
	if err != nil {
		return fmt.Errorf("put block failed: %v", err)
	}

	return err
}

// Get gets the blob object specified by key from a ABS container
func (w *ABS) Get(key string) (io.ReadCloser, error) {
	blobName := path.Join(v1, w.prefix, key)
	blob := w.container.GetBlobReference(blobName)

	opts := &storage.GetBlobOptions{}
	resp, err := blob.Get(opts)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Delete deletes the blob object specified by key from a ABS container
func (w *ABS) Delete(key string) error {
	blobName := path.Join(v1, w.prefix, key)
	blob := w.container.GetBlobReference(blobName)

	opts := &storage.DeleteBlobOptions{}
	err := blob.Delete(opts)

	return err
}

// List lists all blobs in a given ABS container
func (w *ABS) List() ([]string, error) {
	_, l, err := w.list(w.prefix)
	return l, err
}

func (w *ABS) list(prefix string) (int64, []string, error) {
	params := storage.ListBlobsParameters{Prefix: path.Join(v1, prefix) + "/"}
	resp, err := w.container.ListBlobs(params)
	if err != nil {
		return -1, nil, err
	}

	keys := []string{}
	var size int64
	for _, blob := range resp.Blobs {
		k := (blob.Name)[len(resp.Prefix):]
		keys = append(keys, k)
		size += blob.Properties.ContentLength
	}

	return size, keys, nil
}

// TotalSize returns the total size of all blobs in a ABS container
func (w *ABS) TotalSize() (int64, error) {
	size, _, err := w.list(w.prefix)
	return size, err
}

// CopyPrefix copies all blobs with given prefix
func (w *ABS) CopyPrefix(from string) error {
	_, blobs, err := w.list(from)
	if err != nil {
		return err
	}
	for _, blob := range blobs {
		blobResource := w.container.GetBlobReference(blob)

		opts := storage.CopyOptions{}
		if err = blobResource.Copy(path.Join(w.container.Name, v1, from, blob), &opts); err != nil {
			return err
		}
	}
	return nil
}

func randBytes(n int) []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return b
}
