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

package backup

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/coreos/etcd-operator/pkg/backup/abs"
)

var (
	accountName    = storage.StorageEmulatorAccountName
	accountKey     = storage.StorageEmulatorAccountKey
	DefaultBaseURL = "http://127.0.0.1:10000"
	container      = "testcontainer"
	prefix         = "testprefix"
)

// TODO: setup and cleanup blocks

func TestABSBackendContainerDoesNotExist(t *testing.T) {
	t.Fatal()
}
func TestABSBackendGetLatest(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TEST") != "true" {
		t.Skip("skipping integration test due to RUN_INTEGRATION_TEST not set")
	}

	storageClient, err := storage.NewClient(accountName, accountKey, DefaultBaseURL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	blobServiceClient := storageClient.GetBlobService()

	// Create container
	cnt := blobServiceClient.GetContainerReference(container)
	options := storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	}
	_, err = cnt.CreateIfNotExists(&options)
	if err != nil {
		if accountName == storage.StorageEmulatorAccountName {
			t.Fatal(err, "Create container failed: If you are running with the emulator credentials, plaase make sure you have started the azurite storage emulator.")
		}
		t.Fatal(err, "Create container failed")
	}

	abs, err := abs.NewFromClient(container, prefix, &storageClient)
	if err != nil {
		t.Fatal(err)
	}
	ab := &absBackend{ABS: abs}

	if _, err := ab.save("3.1.0", 1, bytes.Neabuffer([]byte("ignore"))); err != nil {
		t.Fatal(err)
	}
	if _, err := ab.save("3.1.1", 2, bytes.Neabuffer([]byte("ignore"))); err != nil {
		t.Fatal(err)
	}

	name, err := ab.getLatest()
	if err != nil {
		t.Fatal(err)
	}

	rc, err := ab.open(name)
	if err != nil {
		t.Fatal(err)
	}

	expected := makeBackupName("3.1.1", 2)
	if name != expected {
		t.Errorf("lastest name = %s, want %s", name, expected)
	}

	b, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	if string(b) != expected {
		t.Errorf("content = %s, want %s", string(b), expected)
	}

	// Delete container
	opts := storage.DeleteContainerOptions{}
	if err := cnt.Delete(&opts); err != nil {
		t.Fatal(err)
	}
}

func TestABSBackendPurge(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TEST") != "true" {
		t.Skip("skipping integration test due to RUN_INTEGRATION_TEST not set")
	}
	storageClient, err := storage.NewClient(accountName, accountKey, DefaultBaseURL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	blobServiceClient := storageClient.GetBlobService()

	// Create container
	cnt := blobServiceClient.GetContainerReference(container)
	options := storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	}
	_, err = cnt.CreateIfNotExists(&options)
	if err != nil {
		if accountName == storage.StorageEmulatorAccountName {
			t.Fatal(err, "Create container failed: If you are running with the emulator credentials, plaase make sure you have started the azurite storage emulator.")
		}
		t.Fatal(err, "Create container failed")
	}

	abs, err := abs.NewFromClient(container, prefix, &storageClient)
	if err != nil {
		t.Fatal(err)
	}
	ab := &absBackend{ABS: abs}

	if _, err := ab.save("3.1.0", 1, bytes.Neabuffer([]byte("ignore"))); err != nil {
		t.Fatal(err)
	}
	if _, err := ab.save("3.1.0", 2, bytes.Neabuffer([]byte("ignore"))); err != nil {
		t.Fatal(err)
	}
	if err := ab.purge(1); err != nil {
		t.Fatal(err)
	}
	names, err := abs.List()
	if err != nil {
		t.Fatal(err)
	}
	leftFiles := []string{makeBackupName("3.1.0", 2)}
	if !reflect.DeepEqual(leftFiles, names) {
		t.Errorf("left files after purge, want=%v, get=%v", leftFiles, names)
	}
	if err := abs.Delete(makeBackupName("3.1.0", 2)); err != nil {
		t.Fatal(err)
	}

	// Delete container
	opts := storage.DeleteContainerOptions{}
	if err := cnt.Delete(&opts); err != nil {
		t.Fatal(err)
	}
}
