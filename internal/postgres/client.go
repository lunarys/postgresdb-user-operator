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
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// PGClient defines the interface for PostgreSQL provisioning operations.
// This allows the controller to be tested with a mock implementation.
type PGClient interface {
	Close(ctx context.Context)
	UserExists(ctx context.Context, username string) (bool, error)
	CreateUser(ctx context.Context, username, password, provenance string) error
	UpdatePassword(ctx context.Context, username, password string) error
	DatabaseExists(ctx context.Context, dbname string) (bool, error)
	CreateDatabase(ctx context.Context, dbname, owner, provenance string) error
	EnsureDatabaseOwner(ctx context.Context, dbname, owner string) error
	CheckUserProvenance(ctx context.Context, username, expectedProvenance string) error
	CheckDatabaseProvenance(ctx context.Context, dbname, expectedProvenance string) error
	DropDatabase(ctx context.Context, dbname string) error
	DropUser(ctx context.Context, username string) error
}

// Client wraps a PostgreSQL connection for provisioning operations.
type Client struct {
	conn *pgx.Conn
}

// NewClient connects to PostgreSQL using the given connection string.
func NewClient(ctx context.Context, connString string) (*Client, error) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	return &Client{conn: conn}, nil
}

// Close closes the underlying connection.
func (c *Client) Close(ctx context.Context) {
	_ = c.conn.Close(ctx)
}

// UserExists checks if a PostgreSQL user exists.
func (c *Client) UserExists(ctx context.Context, username string) (bool, error) {
	var exists bool
	err := c.conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)", username,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking if user %q exists: %w", username, err)
	}
	return exists, nil
}

// CreateUser creates a new PostgreSQL user with LOGIN and the given password,
// and tags it with a provenance comment.
func (c *Client) CreateUser(ctx context.Context, username, password, provenance string) error {
	sql := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD %s",
		quoteIdentifier(username), quoteLiteral(password))
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("creating user %q: %w", username, err)
	}
	return c.SetUserComment(ctx, username, provenance)
}

// UpdatePassword changes the password for an existing user.
func (c *Client) UpdatePassword(ctx context.Context, username, password string) error {
	sql := fmt.Sprintf("ALTER ROLE %s WITH PASSWORD %s",
		quoteIdentifier(username), quoteLiteral(password))
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("updating password for user %q: %w", username, err)
	}
	return nil
}

// DatabaseExists checks if a PostgreSQL database exists.
func (c *Client) DatabaseExists(ctx context.Context, dbname string) (bool, error) {
	var exists bool
	err := c.conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbname,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking if database %q exists: %w", dbname, err)
	}
	return exists, nil
}

// CreateDatabase creates a new database owned by the given user,
// and tags it with a provenance comment.
func (c *Client) CreateDatabase(ctx context.Context, dbname, owner, provenance string) error {
	sql := fmt.Sprintf("CREATE DATABASE %s OWNER %s",
		quoteIdentifier(dbname), quoteIdentifier(owner))
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("creating database %q: %w", dbname, err)
	}
	return c.SetDatabaseComment(ctx, dbname, provenance)
}

// EnsureDatabaseOwner verifies and corrects the ownership of a database.
func (c *Client) EnsureDatabaseOwner(ctx context.Context, dbname, owner string) error {
	var currentOwner string
	err := c.conn.QueryRow(ctx,
		"SELECT pg_catalog.pg_get_userbyid(d.datdba) FROM pg_catalog.pg_database d WHERE d.datname = $1",
		dbname,
	).Scan(&currentOwner)
	if err != nil {
		return fmt.Errorf("checking owner of database %q: %w", dbname, err)
	}
	if currentOwner != owner {
		sql := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s",
			quoteIdentifier(dbname), quoteIdentifier(owner))
		if _, err := c.conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("changing owner of database %q to %q: %w", dbname, owner, err)
		}
	}
	return nil
}

// DropDatabase drops a database if it exists.
func (c *Client) DropDatabase(ctx context.Context, dbname string) error {
	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteIdentifier(dbname))
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("dropping database %q: %w", dbname, err)
	}
	return nil
}

// DropUser drops a user if it exists.
func (c *Client) DropUser(ctx context.Context, username string) error {
	sql := fmt.Sprintf("DROP ROLE IF EXISTS %s", quoteIdentifier(username))
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("dropping user %q: %w", username, err)
	}
	return nil
}

// ErrNotOwned is returned when a PostgreSQL resource exists but is not owned by the requesting CR.
type ErrNotOwned struct {
	Resource           string
	Name               string
	ExpectedProvenance string
	ActualComment      string
}

func (e *ErrNotOwned) Error() string {
	return fmt.Sprintf("%s %q already exists and is not managed by this resource (expected provenance %q, got %q)",
		e.Resource, e.Name, e.ExpectedProvenance, e.ActualComment)
}

// GetUserComment returns the comment on a PostgreSQL user, or empty string if none.
func (c *Client) GetUserComment(ctx context.Context, username string) (string, error) {
	var comment *string
	err := c.conn.QueryRow(ctx,
		"SELECT pg_catalog.shobj_description(oid, 'pg_authid') FROM pg_catalog.pg_authid WHERE rolname = $1",
		username,
	).Scan(&comment)
	if err != nil {
		return "", fmt.Errorf("getting comment for user %q: %w", username, err)
	}
	if comment == nil {
		return "", nil
	}
	return *comment, nil
}

// SetUserComment sets a comment on a PostgreSQL user.
func (c *Client) SetUserComment(ctx context.Context, username, comment string) error {
	sql := fmt.Sprintf("COMMENT ON ROLE %s IS %s",
		quoteIdentifier(username), quoteLiteral(comment))
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("setting comment on user %q: %w", username, err)
	}
	return nil
}

// GetDatabaseComment returns the comment on a PostgreSQL database, or empty string if none.
func (c *Client) GetDatabaseComment(ctx context.Context, dbname string) (string, error) {
	var comment *string
	err := c.conn.QueryRow(ctx,
		"SELECT pg_catalog.shobj_description(oid, 'pg_database') FROM pg_catalog.pg_database WHERE datname = $1",
		dbname,
	).Scan(&comment)
	if err != nil {
		return "", fmt.Errorf("getting comment for database %q: %w", dbname, err)
	}
	if comment == nil {
		return "", nil
	}
	return *comment, nil
}

// SetDatabaseComment sets a comment on a PostgreSQL database.
func (c *Client) SetDatabaseComment(ctx context.Context, dbname, comment string) error {
	sql := fmt.Sprintf("COMMENT ON DATABASE %s IS %s",
		quoteIdentifier(dbname), quoteLiteral(comment))
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("setting comment on database %q: %w", dbname, err)
	}
	return nil
}

// CheckUserProvenance verifies that an existing user is owned by the expected CR.
// Returns nil if the provenance matches, ErrNotOwned if it doesn't.
func (c *Client) CheckUserProvenance(ctx context.Context, username, expectedProvenance string) error {
	comment, err := c.GetUserComment(ctx, username)
	if err != nil {
		return err
	}
	if comment != expectedProvenance {
		return &ErrNotOwned{
			Resource:           "user",
			Name:               username,
			ExpectedProvenance: expectedProvenance,
			ActualComment:      comment,
		}
	}
	return nil
}

// CheckDatabaseProvenance verifies that an existing database is owned by the expected CR.
// Returns nil if the provenance matches, ErrNotOwned if it doesn't.
func (c *Client) CheckDatabaseProvenance(ctx context.Context, dbname, expectedProvenance string) error {
	comment, err := c.GetDatabaseComment(ctx, dbname)
	if err != nil {
		return err
	}
	if comment != expectedProvenance {
		return &ErrNotOwned{
			Resource:           "database",
			Name:               dbname,
			ExpectedProvenance: expectedProvenance,
			ActualComment:      comment,
		}
	}
	return nil
}

// quoteIdentifier quotes a PostgreSQL identifier to prevent SQL injection.
func quoteIdentifier(ident string) string {
	return pgx.Identifier{ident}.Sanitize()
}

// quoteLiteral quotes a PostgreSQL string literal to prevent SQL injection.
func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, "'", "''")
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return "'" + s + "'"
}
