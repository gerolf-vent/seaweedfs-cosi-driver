/*
Copyright 2023 SUSE, LLC.
Copyright 2024 s3gw contributors.
Copyright 2024 SeaweedFS contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
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
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	cosispec "sigs.k8s.io/container-object-storage-interface-spec"
)

/* -------------------------------------------------------------------------- */
/*                               type & helpers                               */
/* -------------------------------------------------------------------------- */

const (
	// parameterPrefix is the namespace prefix for SeaweedFS-specific BucketClass parameters
	parameterPrefix = "seaweedfs.objectstorage.k8s.io/"
)

var (
	replicationParameterRegex = regexp.MustCompile("^[0-9]{3}$")
	diskTypeParameterRegex    = regexp.MustCompile("^[a-z0-9]+$")
	ttlParameterRegex         = regexp.MustCompile("^[0-9]+[mhdwMy]$")
	safeNameRegex             = regexp.MustCompile("^[a-zA-Z0-9._-]{1,253}$")
)

// provisionerServer implements cosi.ProvisionerServer.
type provisionerServer struct {
	provisioner      string
	filerBucketsPath string
	endpoint         string
	region           string
	filerEndpoint    string
	grpcDialOption   grpc.DialOption
}

var _ cosispec.ProvisionerServer = (*provisionerServer)(nil)

// createFilerClient returns a fresh gRPC conn + typed client.
func createFilerClient(ctx context.Context, ep string, opt grpc.DialOption) (*grpc.ClientConn, filer_pb.SeaweedFilerClient, error) {
	conn, err := grpc.DialContext(ctx, ep, opt)
	if err != nil {
		return nil, nil, err
	}
	return conn, filer_pb.NewSeaweedFilerClient(conn), nil
}

// getFilerBucketsPath returns the directory path used for buckets.
func getFilerBucketsPath() string { return "/buckets" }

/* -------------------------------------------------------------------------- */
/*                         constructor & connection pool                      */
/* -------------------------------------------------------------------------- */

func NewProvisionerServer(prov, filerEP, endpoint, region string, opt grpc.DialOption) (cosispec.ProvisionerServer, error) {
	return &provisionerServer{
		provisioner:      prov,
		filerBucketsPath: getFilerBucketsPath(),
		endpoint:         endpoint,
		region:           region,
		filerEndpoint:    filerEP,
		grpcDialOption:   opt,
	}, nil
}

// withFilerClient opens a short-lived conn, executes fn, closes conn.
func (s *provisionerServer) withFilerClient(ctx context.Context, fn func(filer_pb.SeaweedFilerClient) error) error {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, cli, err := createFilerClient(dialCtx, s.filerEndpoint, s.grpcDialOption)
	if err != nil {
		return fmt.Errorf("dial filer: %w", err)
	}
	defer conn.Close()

	return fn(cli)
}

/* -------------------------------------------------------------------------- */
/*                           bucket primitives                                */
/* -------------------------------------------------------------------------- */

func (s *provisionerServer) createBucket(ctx context.Context, name string) error {
	return s.withFilerClient(ctx, func(c filer_pb.SeaweedFilerClient) error {
		_, err := c.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: s.filerBucketsPath,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					FileMode: uint32(0777 | os.ModeDir),
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
				},
			},
		})
		return err
	})
}

func (s *provisionerServer) deleteBucket(ctx context.Context, id string) error {
	return s.withFilerClient(ctx, func(c filer_pb.SeaweedFilerClient) error {
		_, err := c.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory:            s.filerBucketsPath,
			Name:                 id,
			IsDeleteData:         true,
			IsRecursive:          true,
			IgnoreRecursiveError: true,
		})
		return err
	})
}

type bucketParameters struct {
	Replication       string
	DiskType          string
	TTL               string
	VolumeGrowthCount uint32
	DataCenter        string
	Rack              string
	DataNode          string
}

func (bp *bucketParameters) IsEmpty() bool {
	return bp.Replication == "" &&
		bp.DiskType == "" &&
		bp.TTL == "" &&
		bp.VolumeGrowthCount == 0 &&
		bp.DataCenter == "" &&
		bp.Rack == "" &&
		bp.DataNode == ""
}

func (s *provisionerServer) createBucketWithParameters(ctx context.Context, name string, parameters bucketParameters) error {
	// Fast path: no parameters
	if parameters.IsEmpty() {
		return s.createBucket(ctx, name)
	}

	// Create PathConf for this bucket
	bucketPath := filepath.Join(s.filerBucketsPath, name)

	pathConf := &filer_pb.FilerConf_PathConf{
		LocationPrefix:    bucketPath,
		Collection:        name,
		Replication:       parameters.Replication,
		DiskType:          parameters.DiskType,
		Ttl:               parameters.TTL,
		VolumeGrowthCount: parameters.VolumeGrowthCount,
		DataCenter:        parameters.DataCenter,
		Rack:              parameters.Rack,
		DataNode:          parameters.DataNode,
	}

	// Sync PathConf to filer.conf
	if err := s.addFilerPathConf(ctx, pathConf); err != nil {
		return fmt.Errorf("failed to add bucket storage parameters: %w", err)
	}

	// Finally, create the bucket directory
	if err := s.createBucket(ctx, name); err != nil {
		return fmt.Errorf("failed to create bucket directory: %w", err)
	}

	return nil
}

/* -------------------------------------------------------------------------- */
/*                          filer.conf PathConf management                    */
/* -------------------------------------------------------------------------- */

func (s *provisionerServer) readFilerConf(client filer_pb.SeaweedFilerClient) (*filer.FilerConf, error) {
	data, err := filer.ReadInsideFiler(client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName)
	if errors.Is(err, filer_pb.ErrNotFound) {
		return filer.NewFilerConf(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s/%s: %v", filer.DirectoryEtcSeaweedFS, filer.FilerConfName, err)
	}

	fc := filer.NewFilerConf()
	if len(data) > 0 {
		if err := fc.LoadFromBytes(data); err != nil {
			return nil, fmt.Errorf("parse %s/%s: %v", filer.DirectoryEtcSeaweedFS, filer.FilerConfName, err)
		}
	}
	return fc, nil
}

// addFilerPathConf adds or updates a PathConf entry in filer.conf.
func (s *provisionerServer) addFilerPathConf(ctx context.Context, pathConf *filer_pb.FilerConf_PathConf) error {
	return s.withFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
		// Read current filer configuration
		fc, err := s.readFilerConf(client)
		if err != nil {
			return fmt.Errorf("failed to read filer conf: %w", err)
		}

		// Add the PathConf (merges with existing if present)
		if err := fc.AddLocationConf(pathConf); err != nil {
			return fmt.Errorf("failed to add location conf: %w", err)
		}

		// Serialize to JSON
		var buf bytes.Buffer
		if err := fc.ToText(&buf); err != nil {
			return fmt.Errorf("failed to serialize filer conf: %w", err)
		}

		// Save back to filer
		if err := filer.SaveInsideFiler(
			client,
			filer.DirectoryEtcSeaweedFS, // "/etc/seaweedfs"
			filer.FilerConfName,         // "filer.conf"
			buf.Bytes(),
		); err != nil {
			return fmt.Errorf("failed to save filer conf: %w", err)
		}

		klog.InfoS("configured bucket storage parameters",
			"bucket", pathConf.LocationPrefix,
			"replication", pathConf.Replication,
			"diskType", pathConf.DiskType,
			"ttl", pathConf.Ttl,
			"volumeGrowthCount", pathConf.VolumeGrowthCount)

		return nil
	})
}

// removeFilerPathConf removes a PathConf entry from filer.conf.
func (s *provisionerServer) removeFilerPathConf(ctx context.Context, bucketName string) error {
	bucketPath := filepath.Join(s.filerBucketsPath, bucketName)

	return s.withFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
		// Read current filer configuration
		fc, err := s.readFilerConf(client)
		if err != nil {
			return fmt.Errorf("failed to read filer conf: %w", err)
		}

		// Delete the PathConf
		fc.DeleteLocationConf(bucketPath)

		// Serialize and save
		var buf bytes.Buffer
		if err := fc.ToText(&buf); err != nil {
			return fmt.Errorf("failed to serialize filer conf: %w", err)
		}

		if err := filer.SaveInsideFiler(
			client,
			filer.DirectoryEtcSeaweedFS,
			filer.FilerConfName,
			buf.Bytes(),
		); err != nil {
			return fmt.Errorf("failed to save filer conf: %w", err)
		}

		klog.InfoS("removed bucket storage parameters", "bucket", bucketPath)
		return nil
	})
}

/* -------------------------------------------------------------------------- */
/*                                 COSI RPCs                                  */
/* -------------------------------------------------------------------------- */

func (s *provisionerServer) DriverCreateBucket(ctx context.Context, req *cosispec.DriverCreateBucketRequest) (*cosispec.DriverCreateBucketResponse, error) {
	name, parameters := req.GetName(), req.GetParameters()
	klog.InfoS("creating bucket", "name", name, "parameters", parameters)

	// Read and validate parameters
	bucketParameters := bucketParameters{}

	if paramReplication, ok := parameters[parameterPrefix+"replication"]; ok {
		if !replicationParameterRegex.MatchString(paramReplication) {
			klog.ErrorS(nil, "invalid replication parameter", "value", paramReplication, "bucket", name)
			return nil, status.Error(codes.InvalidArgument, "invalid replication parameter: must be empty or 3 digits like '001', '210', '100'")
		}
		bucketParameters.Replication = paramReplication
	}

	if paramDiskType, ok := parameters[parameterPrefix+"diskType"]; ok {
		if !diskTypeParameterRegex.MatchString(paramDiskType) {
			klog.ErrorS(nil, "invalid diskType parameter", "value", paramDiskType, "bucket", name)
			return nil, status.Error(codes.InvalidArgument, "invalid diskType parameter: only lower-case alphanumerical characters allowed")
		}
		bucketParameters.DiskType = paramDiskType
	}

	if paramTTL, ok := parameters[parameterPrefix+"ttl"]; ok {
		if !ttlParameterRegex.MatchString(paramTTL) {
			klog.ErrorS(nil, "invalid ttl parameter", "value", paramTTL, "bucket", name)
			return nil, status.Error(codes.InvalidArgument, "invalid ttl parameter: must be a number followed by m, h, d, w, M, or y")
		}
		bucketParameters.TTL = paramTTL
	}

	if paramVGC, ok := parameters[parameterPrefix+"volumeGrowthCount"]; ok {
		var vgc uint64
		_, err := fmt.Sscanf(paramVGC, "%d", &vgc)
		if err != nil || vgc > 65535 {
			klog.ErrorS(err, "invalid volumeGrowthCount parameter", "value", paramVGC, "bucket", name)
			return nil, status.Error(codes.InvalidArgument, "invalid volumeGrowthCount parameter: must be a number between 0 and 65535")
		}
		bucketParameters.VolumeGrowthCount = uint32(vgc)
	}

	if paramDC, ok := parameters[parameterPrefix+"dataCenter"]; ok {
		if !safeNameRegex.MatchString(paramDC) {
			klog.ErrorS(nil, "invalid dataCenter parameter", "value", paramDC, "bucket", name)
			return nil, status.Error(codes.InvalidArgument, "invalid dataCenter parameter: only alphanumerical characters, dot, underscore, hyphen allowed, max length 253")
		}
		bucketParameters.DataCenter = paramDC
	}

	if paramRack, ok := parameters[parameterPrefix+"rack"]; ok {
		if !safeNameRegex.MatchString(paramRack) {
			klog.ErrorS(nil, "invalid rack parameter", "value", paramRack, "bucket", name)
			return nil, status.Error(codes.InvalidArgument, "invalid rack parameter: only alphanumerical characters, dot, underscore, hyphen allowed, max length 253")
		}
		bucketParameters.Rack = paramRack
	}

	if paramDN, ok := parameters[parameterPrefix+"dataNode"]; ok {
		if !safeNameRegex.MatchString(paramDN) {
			klog.ErrorS(nil, "invalid dataNode parameter", "value", paramDN, "bucket", name)
			return nil, status.Error(codes.InvalidArgument, "invalid dataNode parameter: only alphanumerical characters, dot, underscore, hyphen allowed, max length 253")
		}
		bucketParameters.DataNode = paramDN
	}

	if err := s.createBucketWithParameters(ctx, req.GetName(), bucketParameters); err != nil {
		klog.ErrorS(err, "create bucket failed")
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.InfoS("created bucket", "name", req.GetName())
	return &cosispec.DriverCreateBucketResponse{BucketId: req.GetName()}, nil
}

func (s *provisionerServer) DriverDeleteBucket(ctx context.Context, req *cosispec.DriverDeleteBucketRequest) (*cosispec.DriverDeleteBucketResponse, error) {
	klog.InfoS("deleting bucket", "name", req.GetBucketId())

	// Delete the bucket directory
	if err := s.deleteBucket(ctx, req.GetBucketId()); err != nil {
		klog.ErrorS(err, "delete bucket failed")
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Remove PathConf (don't fail if this doesn't work)
	if err := s.removeFilerPathConf(ctx, req.GetBucketId()); err != nil {
		klog.ErrorS(err, "failed to remove bucket storage parameters", "bucket", req.GetBucketId())
	}

	klog.InfoS("deleted bucket", "name", req.GetBucketId())
	return &cosispec.DriverDeleteBucketResponse{}, nil
}

func (s *provisionerServer) DriverGrantBucketAccess(ctx context.Context, req *cosispec.DriverGrantBucketAccessRequest) (*cosispec.DriverGrantBucketAccessResponse, error) {
	klog.InfoS("granting bucket access", "user", req.GetName(), "bucket", req.GetBucketId())
	user, bucket := req.GetName(), req.GetBucketId()
	if user == "" || bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "user or bucket empty")
	}

	accessKey, _ := GenerateAccessKeyID()
	secretKey, _ := GenerateSecretAccessKey()

	// read-modify-write IAM configuration
	var cfgBuf bytes.Buffer
	if err := s.readS3Configuration(ctx, &cfgBuf); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cfg := &iam_pb.S3ApiConfiguration{}
	if cfgBuf.Len() > 0 {
		if err := filer.ParseS3ConfigurationFromBytes(cfgBuf.Bytes(), cfg); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	// ensure identity exists
	var id *iam_pb.Identity
	for _, i := range cfg.Identities {
		if i.Name == user {
			id = i
			break
		}
	}
	if id == nil {
		id = &iam_pb.Identity{Name: user}
		cfg.Identities = append(cfg.Identities, id)
	}
	id.Credentials = append(id.Credentials, &iam_pb.Credential{AccessKey: accessKey, SecretKey: secretKey})
	for _, a := range []string{"Read", "Write", "List", "Tagging"} {
		action := fmt.Sprintf("%s:%s", a, bucket)
		if !contains(id.Actions, action) {
			id.Actions = append(id.Actions, action)
		}
	}

	// write back
	cfgBuf.Reset()
	filer.ProtoToText(&cfgBuf, cfg)
	if err := s.saveS3Configuration(ctx, cfgBuf.Bytes()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.InfoS("granted bucket access", "user", user, "bucket", bucket, "accessKey", accessKey)
	return &cosispec.DriverGrantBucketAccessResponse{
		AccountId: user,
		Credentials: map[string]*cosispec.CredentialDetails{
			"s3": {Secrets: map[string]string{
				"accessKeyID":     accessKey,
				"accessSecretKey": secretKey,
				"endpoint":        s.endpoint,
				"region":          s.region,
			}},
		},
	}, nil
}

func (s *provisionerServer) DriverRevokeBucketAccess(ctx context.Context, req *cosispec.DriverRevokeBucketAccessRequest) (*cosispec.DriverRevokeBucketAccessResponse, error) {
	klog.InfoS("revoking bucket access", "user", req.GetAccountId(), "bucket", req.GetBucketId())
	user := req.GetAccountId()
	if user == "" {
		return nil, status.Error(codes.InvalidArgument, "user empty")
	}
	if err := s.revokeBucketAccess(ctx, user); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	klog.InfoS("revoked bucket access", "user", user, "bucket", req.GetBucketId())
	return &cosispec.DriverRevokeBucketAccessResponse{}, nil
}

/* -------------------------------------------------------------------------- */
/*                          IAM read / write helpers                          */
/* -------------------------------------------------------------------------- */

func (s *provisionerServer) readS3Configuration(ctx context.Context, buf *bytes.Buffer) error {
	return s.withFilerClient(ctx, func(c filer_pb.SeaweedFilerClient) error {
		resp, err := c.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: filer.IamConfigDirectory,
			Name:      filer.IamIdentityFile,
		})
		if err != nil && !strings.Contains(err.Error(), "no entry is found") {
			return err
		}
		if resp != nil && resp.Entry != nil && resp.Entry.Content != nil {
			buf.Write(resp.Entry.Content)
		}
		return nil
	})
}

func (s *provisionerServer) saveS3Configuration(ctx context.Context, data []byte) error {
	return s.withFilerClient(ctx, func(c filer_pb.SeaweedFilerClient) error {
		_, err := c.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory: filer.IamConfigDirectory,
			Entry: &filer_pb.Entry{
				Name:        filer.IamIdentityFile,
				Content:     data,
				IsDirectory: false,
			},
		})
		if err == nil || !strings.Contains(err.Error(), "no entry is found") {
			return err
		}
		_, err = c.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: filer.IamConfigDirectory,
			Entry: &filer_pb.Entry{
				Name:        filer.IamIdentityFile,
				Content:     data,
				IsDirectory: false,
			},
		})
		return err
	})
}

func (s *provisionerServer) revokeBucketAccess(ctx context.Context, user string) error {
	return s.configureS3Access(ctx, user, "", "", nil, true)
}

func (s *provisionerServer) configureS3Access(ctx context.Context, user, ak, sk string, actions []string, del bool) error {
	var buf bytes.Buffer
	if err := s.readS3Configuration(ctx, &buf); err != nil {
		return err
	}

	cfg := &iam_pb.S3ApiConfiguration{}
	if buf.Len() > 0 {
		if err := filer.ParseS3ConfigurationFromBytes(buf.Bytes(), cfg); err != nil {
			return err
		}
	}

	// update cfg …
	idx := -1
	for i, id := range cfg.Identities {
		if id.Name == user {
			idx = i
			break
		}
	}

	if del {
		if idx >= 0 {
			cfg.Identities = append(cfg.Identities[:idx], cfg.Identities[idx+1:]...)
		}
	} else {
		if idx == -1 {
			cfg.Identities = append(cfg.Identities, &iam_pb.Identity{Name: user})
			idx = len(cfg.Identities) - 1
		}
		id := cfg.Identities[idx]
		if ak != "" && sk != "" {
			id.Credentials = append(id.Credentials, &iam_pb.Credential{AccessKey: ak, SecretKey: sk})
		}
		for _, a := range actions {
			if !contains(id.Actions, a) {
				id.Actions = append(id.Actions, a)
			}
		}
	}

	buf.Reset()
	filer.ProtoToText(&buf, cfg)
	return s.saveS3Configuration(ctx, buf.Bytes())
}

/* -------------------------------------------------------------------------- */
/*                               utilities                                    */
/* -------------------------------------------------------------------------- */

func GenerateAccessKeyID() (string, error) {
	return randomString(20, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
}

func GenerateSecretAccessKey() (string, error) {
	return randomString(40, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/+")
}

func randomString(n int, charset string) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b), nil
}
