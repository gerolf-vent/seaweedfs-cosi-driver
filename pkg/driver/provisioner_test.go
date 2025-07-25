/*
Copyright 2023 SUSE, LLC.
Copyright 2024 s3gw contributors.
Copyright 2024 SeaweedFS contributors.

Licensed under the Apache License, Version 2.0 (the "License");
You may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	cosispec "sigs.k8s.io/container-object-storage-interface-spec"
)

/* -------------------------------- фейковый Filer ------------------------------ */

type fakeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	iam bytes.Buffer
}

func (f *fakeFiler) CreateEntry(ctx context.Context, in *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if in.Directory == filer.IamConfigDirectory {
		f.iam.Reset()
		f.iam.Write(in.Entry.Content)
	}
	return &filer_pb.CreateEntryResponse{}, nil
}
func (f *fakeFiler) UpdateEntry(ctx context.Context, in *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.iam.Reset()
	f.iam.Write(in.Entry.Content)
	return &filer_pb.UpdateEntryResponse{}, nil
}
func (f *fakeFiler) LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if f.iam.Len() == 0 {
		return nil, fmt.Errorf("no entry is found in filer store")
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Content: f.iam.Bytes()}}, nil
}
func (*fakeFiler) DeleteEntry(context.Context, *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	return &filer_pb.DeleteEntryResponse{}, nil
}

/* ------------------------- helper: real TCP gRPC server ----------------------- */

func newProv(t *testing.T) *provisionerServer {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, &fakeFiler{})
	go srv.Serve(lis)

	p, err := NewProvisionerServer("prov", lis.Addr().String(), "", "", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("init prov: %v", err)
	}
	return p.(*provisionerServer)
}

/* ----------------------------------- tests ----------------------------------- */

func TestDriverGrantBucketAccess(t *testing.T) {
	p := newProv(t)

	cases := []struct {
		name    string
		req     *cosispec.DriverGrantBucketAccessRequest
		wantErr bool
	}{
		{"empty bucket", &cosispec.DriverGrantBucketAccessRequest{Name: "u"}, true},
		{"empty user", &cosispec.DriverGrantBucketAccessRequest{BucketId: "b"}, true},
		{"ok", &cosispec.DriverGrantBucketAccessRequest{BucketId: "b", Name: "u"}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := p.DriverGrantBucketAccess(context.Background(), c.req)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if c.wantErr {
				return
			}
			if resp.AccountId != "u" {
				t.Errorf("AccountId=%s want u", resp.AccountId)
			}
			if resp.Credentials["s3"].Secrets["accessKeyID"] == "" ||
				resp.Credentials["s3"].Secrets["accessSecretKey"] == "" {
				t.Errorf("credentials missing")
			}
		})
	}
}

func TestDriverRevokeBucketAccess(t *testing.T) {
	p := newProv(t)
	_, _ = p.DriverGrantBucketAccess(context.Background(),
		&cosispec.DriverGrantBucketAccessRequest{BucketId: "b", Name: "u"})

	cases := []struct {
		name    string
		req     *cosispec.DriverRevokeBucketAccessRequest
		wantErr bool
	}{
		{"empty user", &cosispec.DriverRevokeBucketAccessRequest{}, true},
		{"ok", &cosispec.DriverRevokeBucketAccessRequest{AccountId: "u"}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := p.DriverRevokeBucketAccess(context.Background(), c.req)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && !reflect.DeepEqual(got, &cosispec.DriverRevokeBucketAccessResponse{}) {
				t.Errorf("unexpected resp=%+v", got)
			}
		})
	}
}
