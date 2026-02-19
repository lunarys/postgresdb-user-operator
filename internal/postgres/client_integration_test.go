//go:build integration

/*
Copyright 2026.

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

package postgres

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"testing"
)

// testConnString is read from the environment in TestMain.
var testConnString string

// testNoCleanup skips all resource cleanup when TEST_NO_CLEANUP is set,
// so you can inspect created users and databases after the tests.
var testNoCleanup bool

func TestMain(m *testing.M) {
	testConnString = os.Getenv("TEST_POSTGRES_URL")
	if testConnString == "" {
		fmt.Fprintln(os.Stderr, "TEST_POSTGRES_URL not set, skipping integration tests")
		os.Exit(0)
	}
	testNoCleanup = os.Getenv("TEST_NO_CLEANUP") != ""

	os.Exit(m.Run())
}

// randomSuffix returns a short random alphanumeric string for unique naming.
func randomSuffix() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[idx.Int64()]
	}
	return string(b)
}

// testName builds a unique resource name like "inttest_abc12345_label".
func testName(label string) string {
	return fmt.Sprintf("inttest_%s_%s", randomSuffix(), label)
}

// mustConnect creates a Client and registers cleanup to close it.
func mustConnect(t *testing.T) *Client {
	t.Helper()
	ctx := context.Background()
	c, err := NewClient(ctx, testConnString)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

// cleanupResource registers a t.Cleanup callback unless TEST_NO_CLEANUP is set.
func cleanupResource(t *testing.T, fn func()) {
	if !testNoCleanup {
		t.Cleanup(fn)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNewClient_BadConnString(t *testing.T) {
	ctx := context.Background()
	_, err := NewClient(ctx, "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1")
	if err == nil {
		t.Fatal("expected error for bad connection string, got nil")
	}
}

func TestUserLifecycle(t *testing.T) {
	c := mustConnect(t)
	ctx := context.Background()
	username := testName("user")
	provenance := "integration-test"

	cleanupResource(t, func() {
		_ = c.DropUser(context.Background(), username)
	})

	// Initially does not exist
	exists, err := c.UserExists(ctx, username)
	if err != nil {
		t.Fatalf("UserExists: %v", err)
	}
	if exists {
		t.Fatal("user should not exist yet")
	}

	// Create
	if err := c.CreateUser(ctx, username, "testpass123", provenance); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Now exists
	exists, err = c.UserExists(ctx, username)
	if err != nil {
		t.Fatalf("UserExists after create: %v", err)
	}
	if !exists {
		t.Fatal("user should exist after creation")
	}

	// Update password (should not error)
	if err := c.UpdatePassword(ctx, username, "newpass456"); err != nil {
		t.Fatalf("UpdatePassword: %v", err)
	}

	// Drop
	if err := c.DropUser(ctx, username); err != nil {
		t.Fatalf("DropUser: %v", err)
	}

	// Gone
	exists, err = c.UserExists(ctx, username)
	if err != nil {
		t.Fatalf("UserExists after drop: %v", err)
	}
	if exists {
		t.Fatal("user should not exist after drop")
	}
}

func TestDatabaseLifecycle(t *testing.T) {
	c := mustConnect(t)
	ctx := context.Background()
	username := testName("owner")
	dbname := testName("db")
	provenance := "integration-test"

	cleanupResource(t, func() {
		_ = c.DropDatabase(context.Background(), dbname)
		_ = c.DropUser(context.Background(), username)
	})

	// Create owner user first
	if err := c.CreateUser(ctx, username, "pass", provenance); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Database does not exist
	exists, err := c.DatabaseExists(ctx, dbname)
	if err != nil {
		t.Fatalf("DatabaseExists: %v", err)
	}
	if exists {
		t.Fatal("database should not exist yet")
	}

	// Create database
	if err := c.CreateDatabase(ctx, dbname, username, provenance); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}

	// Now exists
	exists, err = c.DatabaseExists(ctx, dbname)
	if err != nil {
		t.Fatalf("DatabaseExists after create: %v", err)
	}
	if !exists {
		t.Fatal("database should exist after creation")
	}

	// Drop database
	if err := c.DropDatabase(ctx, dbname); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}

	exists, err = c.DatabaseExists(ctx, dbname)
	if err != nil {
		t.Fatalf("DatabaseExists after drop: %v", err)
	}
	if exists {
		t.Fatal("database should not exist after drop")
	}
}

func TestEnsureDatabaseOwner(t *testing.T) {
	c := mustConnect(t)
	ctx := context.Background()
	user1 := testName("owner1")
	user2 := testName("owner2")
	dbname := testName("ownerdb")
	prov := "integration-test"

	cleanupResource(t, func() {
		_ = c.DropDatabase(context.Background(), dbname)
		_ = c.DropUser(context.Background(), user1)
		_ = c.DropUser(context.Background(), user2)
	})

	if err := c.CreateUser(ctx, user1, "pass", prov); err != nil {
		t.Fatalf("CreateUser user1: %v", err)
	}
	if err := c.CreateUser(ctx, user2, "pass", prov); err != nil {
		t.Fatalf("CreateUser user2: %v", err)
	}
	if err := c.CreateDatabase(ctx, dbname, user1, prov); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}

	// Change owner to user2
	if err := c.EnsureDatabaseOwner(ctx, dbname, user2); err != nil {
		t.Fatalf("EnsureDatabaseOwner: %v", err)
	}

	// Calling again with same owner should be idempotent
	if err := c.EnsureDatabaseOwner(ctx, dbname, user2); err != nil {
		t.Fatalf("EnsureDatabaseOwner idempotent: %v", err)
	}
}

func TestUserProvenance(t *testing.T) {
	c := mustConnect(t)
	ctx := context.Background()
	username := testName("prov")
	provenance := "postgresdb-user-operator:default/my-cr"

	cleanupResource(t, func() {
		_ = c.DropUser(context.Background(), username)
	})

	if err := c.CreateUser(ctx, username, "pass", provenance); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Comment should match provenance
	comment, err := c.GetUserComment(ctx, username)
	if err != nil {
		t.Fatalf("GetUserComment: %v", err)
	}
	if comment != provenance {
		t.Fatalf("expected comment %q, got %q", provenance, comment)
	}

	// CheckUserProvenance should succeed with matching provenance
	if err := c.CheckUserProvenance(ctx, username, provenance); err != nil {
		t.Fatalf("CheckUserProvenance matching: %v", err)
	}

	// CheckUserProvenance should return ErrNotOwned with wrong provenance
	err = c.CheckUserProvenance(ctx, username, "wrong-provenance")
	if err == nil {
		t.Fatal("expected ErrNotOwned for mismatched provenance")
	}
	notOwned, ok := err.(*ErrNotOwned)
	if !ok {
		t.Fatalf("expected *ErrNotOwned, got %T: %v", err, err)
	}
	if notOwned.Resource != "user" {
		t.Fatalf("expected resource 'user', got %q", notOwned.Resource)
	}
	if notOwned.ActualComment != provenance {
		t.Fatalf("expected actual comment %q, got %q", provenance, notOwned.ActualComment)
	}
}

func TestDatabaseProvenance(t *testing.T) {
	c := mustConnect(t)
	ctx := context.Background()
	username := testName("dbprovowner")
	dbname := testName("dbprov")
	provenance := "postgresdb-user-operator:default/my-db"

	cleanupResource(t, func() {
		_ = c.DropDatabase(context.Background(), dbname)
		_ = c.DropUser(context.Background(), username)
	})

	if err := c.CreateUser(ctx, username, "pass", provenance); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := c.CreateDatabase(ctx, dbname, username, provenance); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}

	// Comment should match provenance
	comment, err := c.GetDatabaseComment(ctx, dbname)
	if err != nil {
		t.Fatalf("GetDatabaseComment: %v", err)
	}
	if comment != provenance {
		t.Fatalf("expected comment %q, got %q", provenance, comment)
	}

	// Matching provenance should succeed
	if err := c.CheckDatabaseProvenance(ctx, dbname, provenance); err != nil {
		t.Fatalf("CheckDatabaseProvenance matching: %v", err)
	}

	// Mismatched provenance should return ErrNotOwned
	err = c.CheckDatabaseProvenance(ctx, dbname, "wrong")
	if err == nil {
		t.Fatal("expected ErrNotOwned")
	}
	if _, ok := err.(*ErrNotOwned); !ok {
		t.Fatalf("expected *ErrNotOwned, got %T", err)
	}
}

func TestDropNonexistent(t *testing.T) {
	c := mustConnect(t)
	ctx := context.Background()

	// Dropping a non-existent user/database should not error (IF EXISTS)
	if err := c.DropUser(ctx, "inttest_nonexistent_user_xyz"); err != nil {
		t.Fatalf("DropUser nonexistent: %v", err)
	}

	if err := c.DropDatabase(ctx, "inttest_nonexistent_db_xyz"); err != nil {
		t.Fatalf("DropDatabase nonexistent: %v", err)
	}
}

func TestSpecialCharactersInNames(t *testing.T) {
	c := mustConnect(t)
	ctx := context.Background()
	username := testName("user-with-dash")
	provenance := "test's \"provenance\""

	cleanupResource(t, func() {
		_ = c.DropUser(context.Background(), username)
	})

	if err := c.CreateUser(ctx, username, "pass'word\"test", provenance); err != nil {
		t.Fatalf("CreateUser with special chars: %v", err)
	}

	exists, err := c.UserExists(ctx, username)
	if err != nil {
		t.Fatalf("UserExists: %v", err)
	}
	if !exists {
		t.Fatal("user with special chars should exist")
	}

	comment, err := c.GetUserComment(ctx, username)
	if err != nil {
		t.Fatalf("GetUserComment: %v", err)
	}
	if comment != provenance {
		t.Fatalf("provenance roundtrip failed: got %q, want %q", comment, provenance)
	}
}
