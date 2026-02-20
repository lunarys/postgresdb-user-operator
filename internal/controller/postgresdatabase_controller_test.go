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

package controller

import (
	"context"
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	postgresv1alpha1 "github.com/lunarys/postgresdb-user-provisioner/api/v1alpha1"
	"github.com/lunarys/postgresdb-user-provisioner/internal/postgres"
)

// ---------------------------------------------------------------------------
// Mock PGClient
// ---------------------------------------------------------------------------

type mockPGClient struct {
	mu sync.Mutex

	// Simulated state
	users        map[string]string // username -> password
	databases    map[string]string // dbname -> owner
	userComments map[string]string // username -> comment (provenance)
	dbComments   map[string]string // dbname -> comment (provenance)

	// Call counters
	CreateUserCalls          int
	UpdatePasswordCalls      int
	CreateDatabaseCalls      int
	EnsureDatabaseOwnerCalls int
	DropDatabaseCalls        int
	DropUserCalls            int

	// Injectable errors
	ConnectError        error
	UserExistsError     error
	CreateUserError     error
	DatabaseExistsError error
	CreateDatabaseError error
	DropDatabaseError   error
	DropUserError       error
}

func newMockPGClient() *mockPGClient {
	return &mockPGClient{
		users:        make(map[string]string),
		databases:    make(map[string]string),
		userComments: make(map[string]string),
		dbComments:   make(map[string]string),
	}
}

func (m *mockPGClient) Close(_ context.Context) {}

func (m *mockPGClient) UserExists(_ context.Context, username string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.UserExistsError != nil {
		return false, m.UserExistsError
	}
	_, ok := m.users[username]
	return ok, nil
}

func (m *mockPGClient) CreateUser(_ context.Context, username, password, provenance string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.CreateUserError != nil {
		return m.CreateUserError
	}
	m.CreateUserCalls++
	m.users[username] = password
	m.userComments[username] = provenance
	return nil
}

func (m *mockPGClient) UpdatePassword(_ context.Context, username, password string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpdatePasswordCalls++
	m.users[username] = password
	return nil
}

func (m *mockPGClient) DatabaseExists(_ context.Context, dbname string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DatabaseExistsError != nil {
		return false, m.DatabaseExistsError
	}
	_, ok := m.databases[dbname]
	return ok, nil
}

func (m *mockPGClient) CreateDatabase(_ context.Context, dbname, owner, provenance string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.CreateDatabaseError != nil {
		return m.CreateDatabaseError
	}
	m.CreateDatabaseCalls++
	m.databases[dbname] = owner
	m.dbComments[dbname] = provenance
	return nil
}

func (m *mockPGClient) EnsureDatabaseOwner(_ context.Context, dbname, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.EnsureDatabaseOwnerCalls++
	m.databases[dbname] = owner
	return nil
}

func (m *mockPGClient) CheckUserProvenance(_ context.Context, username, expectedProvenance string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	actual := m.userComments[username]
	if actual != expectedProvenance {
		return &postgres.ErrNotOwned{
			Resource:           "user",
			Name:               username,
			ExpectedProvenance: expectedProvenance,
			ActualComment:      actual,
		}
	}
	return nil
}

func (m *mockPGClient) CheckDatabaseProvenance(_ context.Context, dbname, expectedProvenance string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	actual := m.dbComments[dbname]
	if actual != expectedProvenance {
		return &postgres.ErrNotOwned{
			Resource:           "database",
			Name:               dbname,
			ExpectedProvenance: expectedProvenance,
			ActualComment:      actual,
		}
	}
	return nil
}

func (m *mockPGClient) DropDatabase(_ context.Context, dbname string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DropDatabaseError != nil {
		return m.DropDatabaseError
	}
	m.DropDatabaseCalls++
	delete(m.databases, dbname)
	delete(m.dbComments, dbname)
	return nil
}

func (m *mockPGClient) DropUser(_ context.Context, username string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DropUserError != nil {
		return m.DropUserError
	}
	m.DropUserCalls++
	delete(m.users, username)
	delete(m.userComments, username)
	return nil
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const (
	testClusterName      = "my-cluster"
	testClusterNamespace = "cnpg-system"
	testNamespace        = "default"
)

func clusterRef() *postgresv1alpha1.ClusterReference {
	return &postgresv1alpha1.ClusterReference{
		Name:      testClusterName,
		Namespace: testClusterNamespace,
	}
}

// createSuperuserSecret creates the CNPG superuser secret that the controller expects.
func createSuperuserSecret(ctx context.Context) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterName + "-superuser",
			Namespace: testClusterNamespace,
		},
		Data: map[string][]byte{
			"username": []byte("postgres"),
			"password": []byte("supersecret"),
		},
	}
	// Create the namespace first if needed
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testClusterNamespace}}
	_ = k8sClient.Create(ctx, ns)
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, secret))).To(Succeed())
}

// deleteSuperuserSecret removes the superuser secret.
func deleteSuperuserSecret(ctx context.Context) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterName + "-superuser",
			Namespace: testClusterNamespace,
		},
	}
	_ = client.IgnoreNotFound(k8sClient.Delete(ctx, secret))
}

// createPostgresDatabase creates a PostgresDatabase CR and returns its namespaced name.
func createPostgresDatabase(ctx context.Context, name string, specOverrides ...func(*postgresv1alpha1.PostgresDatabaseSpec)) types.NamespacedName {
	nn := types.NamespacedName{Name: name, Namespace: testNamespace}
	pgdb := &postgresv1alpha1.PostgresDatabase{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: postgresv1alpha1.PostgresDatabaseSpec{
			ClusterRef: clusterRef(),
		},
	}
	for _, fn := range specOverrides {
		fn(&pgdb.Spec)
	}
	Expect(k8sClient.Create(ctx, pgdb)).To(Succeed())
	return nn
}

// deletePostgresDatabase removes a PostgresDatabase CR (ignores not-found).
func deletePostgresDatabase(ctx context.Context, nn types.NamespacedName) {
	pgdb := &postgresv1alpha1.PostgresDatabase{}
	if err := k8sClient.Get(ctx, nn, pgdb); err != nil {
		return
	}
	// Remove finalizer so the resource can be deleted
	pgdb.Finalizers = nil
	_ = k8sClient.Update(ctx, pgdb)
	_ = client.IgnoreNotFound(k8sClient.Delete(ctx, pgdb))
}

// newTestReconciler builds a reconciler wired to the given mock.
func newTestReconciler(mock *mockPGClient, opts ...func(*PostgresDatabaseReconciler)) *PostgresDatabaseReconciler {
	r := &PostgresDatabaseReconciler{
		Client:   k8sClient,
		Scheme:   k8sClient.Scheme(),
		Recorder: record.NewFakeRecorder(20),
		ConnectPG: func(_ context.Context, _ string) (postgres.PGClient, error) {
			if mock.ConnectError != nil {
				return nil, mock.ConnectError
			}
			return mock, nil
		},
	}
	for _, fn := range opts {
		fn(r)
	}
	return r
}

// reconcileOnce calls Reconcile and returns the result.
func reconcileOnce(ctx context.Context, r *PostgresDatabaseReconciler, nn types.NamespacedName) (reconcile.Result, error) {
	return r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
}

// refreshResource re-fetches the CR from the API server.
func refreshResource(ctx context.Context, nn types.NamespacedName) *postgresv1alpha1.PostgresDatabase {
	pgdb := &postgresv1alpha1.PostgresDatabase{}
	ExpectWithOffset(1, k8sClient.Get(ctx, nn, pgdb)).To(Succeed())
	return pgdb
}

// getCondition returns the condition with the given type, or nil.
func getCondition(pgdb *postgresv1alpha1.PostgresDatabase, condType string) *metav1.Condition {
	return meta.FindStatusCondition(pgdb.Status.Conditions, condType)
}

// getOutputSecret fetches the output secret for a given CR name.
func getOutputSecret(ctx context.Context, crName string) *corev1.Secret {
	secret := &corev1.Secret{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      crName + "-pgcreds",
		Namespace: testNamespace,
	}, secret)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return secret
}

// cnpgClusterGVK is the GVK for a CNPG Cluster object.
var cnpgClusterGVK = schema.GroupVersionKind{
	Group:   "postgresql.cnpg.io",
	Version: "v1",
	Kind:    "Cluster",
}

// createCNPGCluster creates a minimal unstructured CNPG Cluster object in envtest
// with the given name, namespace, and labels.
func createCNPGCluster(ctx context.Context, name, namespace string, lbls map[string]string) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	_ = k8sClient.Create(ctx, ns)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(cnpgClusterGVK)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetLabels(lbls)
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, obj))).To(Succeed())
}

// deleteCNPGCluster removes a CNPG Cluster object.
func deleteCNPGCluster(ctx context.Context, name, namespace string) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(cnpgClusterGVK)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	_ = client.IgnoreNotFound(k8sClient.Delete(ctx, obj))
}

// createSuperuserSecretFor creates a CNPG superuser secret for a cluster other than the default test cluster.
func createSuperuserSecretFor(ctx context.Context, clusterName, namespace string) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	_ = k8sClient.Create(ctx, ns)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName + "-superuser",
			Namespace: namespace,
		},
		Data: map[string][]byte{
			"username": []byte("postgres"),
			"password": []byte("supersecret"),
		},
	}
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, secret))).To(Succeed())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

var _ = Describe("PostgresDatabase Controller", func() {

	var (
		ctx  context.Context
		mock *mockPGClient
	)

	BeforeEach(func() {
		ctx = context.Background()
		mock = newMockPGClient()
		createSuperuserSecret(ctx)
	})

	// -----------------------------------------------------------------------
	// Provisioning a new database
	// -----------------------------------------------------------------------
	Describe("Provisioning a new database", func() {

		It("should create PG user, PG database, output secret, finalizer, and report Ready=True", func() {
			nn := createPostgresDatabase(ctx, "basic-test")
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			// PG state
			Expect(mock.CreateUserCalls).To(Equal(1))
			Expect(mock.CreateDatabaseCalls).To(Equal(1))

			// CR state
			pgdb := refreshResource(ctx, nn)
			Expect(pgdb.Finalizers).To(ContainElement(finalizerName))
			Expect(pgdb.Status.DatabaseName).To(Equal("basic_test"))
			Expect(pgdb.Status.Username).To(Equal("basic_test"))
			Expect(pgdb.Status.SecretName).To(Equal("basic-test-pgcreds"))

			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))

			// Output secret exists
			secret := getOutputSecret(ctx, "basic-test")
			Expect(secret.Data["username"]).To(Equal([]byte("basic_test")))
			Expect(secret.Data["dbname"]).To(Equal([]byte("basic_test")))
			Expect(secret.Data["host"]).NotTo(BeEmpty())
		})

		It("should use explicit databaseName, username, and secretName when specified", func() {
			nn := createPostgresDatabase(ctx, "explicit-names", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.DatabaseName = "my_db"
				s.Username = "my_user"
				s.SecretName = "my-secret"
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			pgdb := refreshResource(ctx, nn)
			Expect(pgdb.Status.DatabaseName).To(Equal("my_db"))
			Expect(pgdb.Status.Username).To(Equal("my_user"))
			Expect(pgdb.Status.SecretName).To(Equal("my-secret"))

			Expect(mock.users).To(HaveKey("my_user"))
			Expect(mock.databases).To(HaveKey("my_db"))
		})

		It("should prefix names with namespace when namespace-prefix is enabled", func() {
			nn := createPostgresDatabase(ctx, "prefix-test")
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.NamespacePrefix = true
			})
			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			pgdb := refreshResource(ctx, nn)
			Expect(pgdb.Status.DatabaseName).To(Equal("default_prefix_test"))
			Expect(pgdb.Status.Username).To(Equal("default_prefix_test"))
		})
	})

	// -----------------------------------------------------------------------
	// Cluster reference resolution
	// -----------------------------------------------------------------------
	Describe("Cluster reference resolution", func() {

		It("should use default cluster when clusterRef is omitted", func() {
			nn := createPostgresDatabase(ctx, "default-cluster", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.ClusterRef = nil
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.DefaultClusterName = testClusterName
				r.DefaultClusterNamespace = testClusterNamespace
			})
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		})

		It("should use the default cluster selector when clusterRef is omitted", func() {
			const selectorCluster = "selector-cluster"
			const selectorNamespace = "selector-ns"

			createCNPGCluster(ctx, selectorCluster, selectorNamespace, map[string]string{"env": "selector-test"})
			defer deleteCNPGCluster(ctx, selectorCluster, selectorNamespace)
			createSuperuserSecretFor(ctx, selectorCluster, selectorNamespace)

			nn := createPostgresDatabase(ctx, "selector-cluster-test", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.ClusterRef = nil
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.DefaultClusterSelector = "env=selector-test"
			})
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		})

		It("should use CR's clusterRef when provided", func() {
			var capturedConnString string
			nn := createPostgresDatabase(ctx, "explicit-cluster")
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			r.ConnectPG = func(_ context.Context, connString string) (postgres.PGClient, error) {
				capturedConnString = connString
				return mock, nil
			}

			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			Expect(capturedConnString).To(ContainSubstring(
				fmt.Sprintf("%s-rw.%s.svc.cluster.local", testClusterName, testClusterNamespace)))
		})

		It("should report permanent error when neither clusterRef nor default is set", func() {
			nn := createPostgresDatabase(ctx, "no-cluster", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.ClusterRef = nil
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock) // no defaults configured
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred()) // no error returned to controller-runtime
			Expect(result.RequeueAfter).To(BeZero())
			Expect(result.Requeue).To(BeFalse()) // permanent — don't requeue

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("NoClusterRef"))
		})
	})

	// -----------------------------------------------------------------------
	// Error handling
	// -----------------------------------------------------------------------
	Describe("Error handling", func() {

		It("should retry when superuser secret is missing", func() {
			deleteSuperuserSecret(ctx)

			nn := createPostgresDatabase(ctx, "missing-secret")
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(errorRequeueInterval))

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("SuperuserSecretNotFound"))
		})

		It("should retry when PG connection fails", func() {
			nn := createPostgresDatabase(ctx, "conn-fail")
			defer deletePostgresDatabase(ctx, nn)

			mock.ConnectError = fmt.Errorf("connection refused")
			r := newTestReconciler(mock)
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(errorRequeueInterval))

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("ConnectionFailed"))
		})
	})

	// -----------------------------------------------------------------------
	// Provenance conflicts
	// -----------------------------------------------------------------------
	Describe("Provenance conflicts", func() {

		It("should refuse to touch a user owned by a different CR", func() {
			// Pre-populate mock with a user owned by "other-cr"
			mock.users["conflict_user"] = "somepass"
			mock.userComments["conflict_user"] = "postgresdb-user-operator:other-ns/other-cr"

			nn := createPostgresDatabase(ctx, "conflict-user", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.DatabaseName = "conflict_user"
				s.Username = "conflict_user"
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero()) // permanent, no requeue
			Expect(result.Requeue).To(BeFalse())

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("Conflict"))
		})

		It("should refuse to touch a database owned by a different CR", func() {
			provenance := fmt.Sprintf("postgresdb-user-operator:%s/conflict-db", testNamespace)
			// Pre-populate mock: user is ours, but database belongs to someone else
			mock.users["conflict_db"] = "somepass"
			mock.userComments["conflict_db"] = provenance
			mock.databases["conflict_db"] = "conflict_db"
			mock.dbComments["conflict_db"] = "postgresdb-user-operator:other-ns/other-cr"

			nn := createPostgresDatabase(ctx, "conflict-db", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.DatabaseName = "conflict_db"
				s.Username = "conflict_db"
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
			Expect(result.RequeueAfter).To(BeZero())

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("Conflict"))
		})
	})

	// -----------------------------------------------------------------------
	// Idempotency
	// -----------------------------------------------------------------------
	Describe("Idempotency", func() {

		It("should not recreate resources on second reconcile", func() {
			nn := createPostgresDatabase(ctx, "idempotent")
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(mock.CreateUserCalls).To(Equal(1))
			Expect(mock.CreateDatabaseCalls).To(Equal(1))

			// Second reconcile — resources already exist in mock
			_, err = reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			// Create counts should stay the same
			Expect(mock.CreateUserCalls).To(Equal(1))
			Expect(mock.CreateDatabaseCalls).To(Equal(1))
		})
	})

	// -----------------------------------------------------------------------
	// Password management
	// -----------------------------------------------------------------------
	Describe("Password management", func() {

		It("should reset password when user exists but output secret was deleted", func() {
			nn := createPostgresDatabase(ctx, "pw-reset")
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			// First reconcile — creates everything
			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(mock.CreateUserCalls).To(Equal(1))

			// Delete the output secret (simulating accidental deletion)
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "pw-reset-pgcreds",
				Namespace: testNamespace,
			}, secret)).To(Succeed())
			Expect(k8sClient.Delete(ctx, secret)).To(Succeed())

			// Second reconcile — should update password, not create user
			_, err = reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(mock.CreateUserCalls).To(Equal(1))     // no new CreateUser
			Expect(mock.UpdatePasswordCalls).To(Equal(1)) // password was reset
		})
	})

	// -----------------------------------------------------------------------
	// Deletion
	// -----------------------------------------------------------------------
	Describe("Deletion", func() {

		It("DeletionPolicy=Delete should drop DB and user, remove finalizer", func() {
			nn := createPostgresDatabase(ctx, "delete-policy", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.DeletionPolicy = postgresv1alpha1.DeletionPolicyDelete
			})

			r := newTestReconciler(mock)
			// First reconcile — provisions everything
			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			// Mark for deletion
			pgdb := refreshResource(ctx, nn)
			Expect(k8sClient.Delete(ctx, pgdb)).To(Succeed())

			// Reconcile the deletion
			_, err = reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			Expect(mock.DropDatabaseCalls).To(Equal(1))
			Expect(mock.DropUserCalls).To(Equal(1))

			// Resource should be gone (finalizer removed, K8s deletes it)
			gone := &postgresv1alpha1.PostgresDatabase{}
			err = k8sClient.Get(ctx, nn, gone)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("DeletionPolicy=Retain should not drop anything, still remove finalizer", func() {
			nn := createPostgresDatabase(ctx, "retain-policy", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.DeletionPolicy = postgresv1alpha1.DeletionPolicyRetain
			})

			r := newTestReconciler(mock)
			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			pgdb := refreshResource(ctx, nn)
			Expect(k8sClient.Delete(ctx, pgdb)).To(Succeed())

			_, err = reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			Expect(mock.DropDatabaseCalls).To(Equal(0))
			Expect(mock.DropUserCalls).To(Equal(0))

			gone := &postgresv1alpha1.PostgresDatabase{}
			err = k8sClient.Get(ctx, nn, gone)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	// -----------------------------------------------------------------------
	// Output secret format
	// -----------------------------------------------------------------------
	Describe("Output secret format", func() {

		It("should produce a CNPG-compatible secret with all expected keys and owner reference", func() {
			nn := createPostgresDatabase(ctx, "secret-format")
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock)
			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			secret := getOutputSecret(ctx, "secret-format")

			// Type
			Expect(secret.Type).To(Equal(corev1.SecretTypeBasicAuth))

			// All expected keys
			for _, key := range []string{"username", "password", "host", "port", "dbname", "uri", "jdbc-uri", "pgpass"} {
				Expect(secret.Data).To(HaveKey(key), "missing key: "+key)
				Expect(secret.Data[key]).NotTo(BeEmpty(), "empty key: "+key)
			}

			// Values
			Expect(string(secret.Data["username"])).To(Equal("secret_format"))
			Expect(string(secret.Data["dbname"])).To(Equal("secret_format"))
			Expect(string(secret.Data["host"])).To(Equal(
				fmt.Sprintf("%s-rw.%s.svc.cluster.local", testClusterName, testClusterNamespace)))
			Expect(string(secret.Data["port"])).To(Equal("5432"))

			// URI format checks
			Expect(string(secret.Data["uri"])).To(HavePrefix("postgresql://"))
			Expect(string(secret.Data["jdbc-uri"])).To(HavePrefix("jdbc:postgresql://"))

			// pgpass format: host:port:dbname:username:password
			Expect(string(secret.Data["pgpass"])).To(ContainSubstring(":5432:secret_format:secret_format:"))

			// Owner reference
			Expect(secret.OwnerReferences).To(HaveLen(1))
			Expect(secret.OwnerReferences[0].Kind).To(Equal("PostgresDatabase"))
			Expect(secret.OwnerReferences[0].Name).To(Equal("secret-format"))

			// Labels
			Expect(secret.Labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "postgresdb-user-operator"))
			Expect(secret.Labels).To(HaveKeyWithValue("postgres.crds.lunarys.lab/postgresdatabase", "secret-format"))
		})
	})

	// -----------------------------------------------------------------------
	// Default cluster selector resolution
	// -----------------------------------------------------------------------
	Describe("Default cluster selector resolution", func() {

		const selectorClusterName = "selector-cluster"
		const selectorNamespace = "selector-ns"

		It("should resolve the cluster when exactly one match is found", func() {
			createCNPGCluster(ctx, selectorClusterName, selectorNamespace, map[string]string{"env": "prod"})
			defer deleteCNPGCluster(ctx, selectorClusterName, selectorNamespace)
			createSuperuserSecretFor(ctx, selectorClusterName, selectorNamespace)

			nn := createPostgresDatabase(ctx, "selector-one-match", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.ClusterRef = nil
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.DefaultClusterSelector = "env=prod"
				r.DefaultClusterNamespace = selectorNamespace
			})
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		})

		It("should scope the selector search to DefaultClusterNamespace, ignoring clusters in other namespaces", func() {
			// Target cluster in selectorNamespace
			createCNPGCluster(ctx, selectorClusterName, selectorNamespace, map[string]string{"env": "prod"})
			defer deleteCNPGCluster(ctx, selectorClusterName, selectorNamespace)
			createSuperuserSecretFor(ctx, selectorClusterName, selectorNamespace)

			// Decoy cluster in another namespace with the same label
			createCNPGCluster(ctx, "decoy-cluster", "default", map[string]string{"env": "prod"})
			defer deleteCNPGCluster(ctx, "decoy-cluster", "default")

			nn := createPostgresDatabase(ctx, "selector-scoped", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.ClusterRef = nil
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.DefaultClusterSelector = "env=prod"
				r.DefaultClusterNamespace = selectorNamespace // restricts search; decoy is excluded
			})
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		})

		It("should report a permanent error when the selector matches zero clusters", func() {
			nn := createPostgresDatabase(ctx, "selector-no-match", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.ClusterRef = nil
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.DefaultClusterSelector = "env=nonexistent"
			})
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
			Expect(result.RequeueAfter).To(BeZero())

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("NoClusterRef"))
			Expect(readyCond.Message).To(ContainSubstring("no CNPG cluster found"))
		})

		It("should report a permanent error when the selector matches more than one cluster", func() {
			createCNPGCluster(ctx, "cluster-a", selectorNamespace, map[string]string{"env": "prod"})
			defer deleteCNPGCluster(ctx, "cluster-a", selectorNamespace)
			createCNPGCluster(ctx, "cluster-b", selectorNamespace, map[string]string{"env": "prod"})
			defer deleteCNPGCluster(ctx, "cluster-b", selectorNamespace)

			nn := createPostgresDatabase(ctx, "selector-ambiguous", func(s *postgresv1alpha1.PostgresDatabaseSpec) {
				s.ClusterRef = nil
			})
			defer deletePostgresDatabase(ctx, nn)

			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.DefaultClusterSelector = "env=prod"
				r.DefaultClusterNamespace = selectorNamespace
			})
			result, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
			Expect(result.RequeueAfter).To(BeZero())

			pgdb := refreshResource(ctx, nn)
			readyCond := getCondition(pgdb, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("NoClusterRef"))
			Expect(readyCond.Message).To(ContainSubstring("must be unambiguous"))
		})

		It("should prefer spec.clusterRef over the selector when both are configured", func() {
			// Selector cluster exists in selectorNamespace
			createCNPGCluster(ctx, selectorClusterName, selectorNamespace, map[string]string{"env": "prod"})
			defer deleteCNPGCluster(ctx, selectorClusterName, selectorNamespace)

			// CR has an explicit clusterRef pointing at testClusterName
			nn := createPostgresDatabase(ctx, "selector-explicit-wins") // uses default clusterRef()
			defer deletePostgresDatabase(ctx, nn)

			var capturedConnString string
			r := newTestReconciler(mock, func(r *PostgresDatabaseReconciler) {
				r.DefaultClusterSelector = "env=prod"
				orig := r.ConnectPG
				r.ConnectPG = func(ctx context.Context, connString string) (postgres.PGClient, error) {
					capturedConnString = connString
					return orig(ctx, connString)
				}
			})

			_, err := reconcileOnce(ctx, r, nn)
			Expect(err).NotTo(HaveOccurred())

			// Connection string must use testClusterName (from spec.clusterRef), not selectorClusterName
			Expect(capturedConnString).To(ContainSubstring(
				fmt.Sprintf("%s-rw.%s.svc.cluster.local", testClusterName, testClusterNamespace)))
			Expect(capturedConnString).NotTo(ContainSubstring(selectorClusterName))
		})
	})
})
