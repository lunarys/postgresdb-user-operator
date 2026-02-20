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
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	postgresv1alpha1 "github.com/lunarys/postgresdb-user-provisioner/api/v1alpha1"
	"github.com/lunarys/postgresdb-user-provisioner/internal/postgres"

	"errors"
)

const (
	finalizerName = "postgres.crds.lunarys.lab/cleanup"
	passwordLen   = 48

	requeueInterval      = 5 * time.Minute
	errorRequeueInterval = 30 * time.Second

	conditionReady         = "Ready"
	conditionUserReady     = "UserReady"
	conditionDatabaseReady = "DatabaseReady"
	conditionSecretReady   = "SecretReady"
)

// ConnectFunc creates a PGClient from a connection string.
type ConnectFunc func(ctx context.Context, connString string) (postgres.PGClient, error)

// PostgresDatabaseReconciler reconciles a PostgresDatabase object.
type PostgresDatabaseReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	ConnectPG               ConnectFunc
	NamespacePrefix         bool
	DefaultClusterName      string
	DefaultClusterNamespace string
	DefaultClusterSelector  string
}

// +kubebuilder:rbac:groups=postgres.crds.lunarys.lab,resources=postgresdatabases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgres.crds.lunarys.lab,resources=postgresdatabases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=postgres.crds.lunarys.lab,resources=postgresdatabases/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch

func (r *PostgresDatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Fetch the CR
	pgdb := &postgresv1alpha1.PostgresDatabase{}
	if err := r.Get(ctx, req.NamespacedName, pgdb); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Resolve effective cluster reference
	clusterRef, err := r.resolveClusterRef(ctx, pgdb)
	if err != nil {
		log.Error(err, "no cluster reference configured")
		r.setNotReadyCondition(ctx, pgdb, "NoClusterRef", err.Error())
		return ctrl.Result{}, nil // permanent error — don't requeue
	}

	// Resolve effective names
	dbName := r.resolvedDatabaseName(pgdb)
	userName := r.resolvedUsername(pgdb)
	secretName := resolvedSecretName(pgdb)

	// 2. Handle deletion
	if !pgdb.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(pgdb, finalizerName) {
			if pgdb.Spec.DeletionPolicy == postgresv1alpha1.DeletionPolicyDelete {
				if err := r.cleanupPostgresResources(ctx, clusterRef, dbName, userName); err != nil {
					log.Error(err, "failed to clean up PostgreSQL resources, will retry")
					r.Recorder.Eventf(pgdb, corev1.EventTypeWarning, "CleanupFailed",
						"Failed to drop database/user: %v", err)
					meta.SetStatusCondition(&pgdb.Status.Conditions, metav1.Condition{
						Type:    conditionReady,
						Status:  metav1.ConditionFalse,
						Reason:  "CleanupFailed",
						Message: fmt.Sprintf("Failed to drop database/user: %v", err),
					})
					_ = r.Status().Update(ctx, pgdb)
					return ctrl.Result{RequeueAfter: errorRequeueInterval}, nil
				}
				r.Recorder.Event(pgdb, corev1.EventTypeNormal, "CleanedUp",
					"PostgreSQL database and user dropped")
			}

			controllerutil.RemoveFinalizer(pgdb, finalizerName)
			if err := r.Update(ctx, pgdb); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// 3. Add finalizer
	if !controllerutil.ContainsFinalizer(pgdb, finalizerName) {
		controllerutil.AddFinalizer(pgdb, finalizerName)
		if err := r.Update(ctx, pgdb); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Connect to PostgreSQL
	pgClient, host, port, connectErr := r.connectToCluster(ctx, clusterRef)
	if connectErr != nil {
		if apierrors.IsNotFound(connectErr) {
			suSecretName := clusterRef.Name + "-superuser"
			log.Info("superuser secret not found, will retry",
				"secret", suSecretName, "namespace", clusterRef.Namespace)
			r.setNotReadyCondition(ctx, pgdb, "SuperuserSecretNotFound",
				fmt.Sprintf("Secret %s/%s not found", clusterRef.Namespace, suSecretName))
			r.Recorder.Eventf(pgdb, corev1.EventTypeWarning, "SuperuserSecretNotFound",
				"Superuser secret %s/%s not found", clusterRef.Namespace, suSecretName)
			return ctrl.Result{RequeueAfter: errorRequeueInterval}, nil
		}
		log.Error(connectErr, "failed to connect to PostgreSQL")
		r.setNotReadyCondition(ctx, pgdb, "ConnectionFailed",
			fmt.Sprintf("Failed to connect to PostgreSQL: %v", connectErr))
		r.Recorder.Eventf(pgdb, corev1.EventTypeWarning, "ConnectionFailed",
			"Failed to connect to PostgreSQL cluster %s/%s: %v",
			clusterRef.Namespace, clusterRef.Name, connectErr)
		return ctrl.Result{RequeueAfter: errorRequeueInterval}, nil
	}
	defer pgClient.Close(ctx)

	// Provenance tag to track ownership of PG resources
	provenance := fmt.Sprintf("postgresdb-user-operator:%s/%s", pgdb.Namespace, pgdb.Name)

	// 6. Provision user
	password, err := r.ensureUser(ctx, pgdb, pgClient, userName, secretName, provenance)
	if err != nil {
		var notOwned *postgres.ErrNotOwned
		if errors.As(err, &notOwned) {
			log.Error(err, "resource conflict", "username", userName)
			r.setNotReadyCondition(ctx, pgdb, "Conflict", err.Error())
			r.Recorder.Eventf(pgdb, corev1.EventTypeWarning, "Conflict", "%v", err)
			return ctrl.Result{}, nil // permanent error — don't requeue
		}
		log.Error(err, "failed to provision user", "username", userName)
		r.setNotReadyCondition(ctx, pgdb, "UserProvisionFailed",
			fmt.Sprintf("Failed to provision user %q: %v", userName, err))
		return ctrl.Result{RequeueAfter: errorRequeueInterval}, nil
	}
	meta.SetStatusCondition(&pgdb.Status.Conditions, metav1.Condition{
		Type:    conditionUserReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Provisioned",
		Message: fmt.Sprintf("User %q exists", userName),
	})

	// 7. Provision database
	if err := r.ensureDatabase(ctx, pgClient, dbName, userName, provenance); err != nil {
		var notOwned *postgres.ErrNotOwned
		if errors.As(err, &notOwned) {
			log.Error(err, "resource conflict", "database", dbName)
			r.setNotReadyCondition(ctx, pgdb, "Conflict", err.Error())
			r.Recorder.Eventf(pgdb, corev1.EventTypeWarning, "Conflict", "%v", err)
			return ctrl.Result{}, nil // permanent error — don't requeue
		}
		log.Error(err, "failed to provision database", "database", dbName)
		r.setNotReadyCondition(ctx, pgdb, "DatabaseProvisionFailed",
			fmt.Sprintf("Failed to provision database %q: %v", dbName, err))
		return ctrl.Result{RequeueAfter: errorRequeueInterval}, nil
	}
	meta.SetStatusCondition(&pgdb.Status.Conditions, metav1.Condition{
		Type:    conditionDatabaseReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Provisioned",
		Message: fmt.Sprintf("Database %q exists with owner %q", dbName, userName),
	})

	// 8. Create/update output secret
	if err := r.ensureSecret(ctx, pgdb, host, port, dbName, userName, password); err != nil {
		log.Error(err, "failed to ensure output secret")
		r.setNotReadyCondition(ctx, pgdb, "SecretFailed",
			fmt.Sprintf("Failed to create/update secret: %v", err))
		return ctrl.Result{RequeueAfter: errorRequeueInterval}, nil
	}
	meta.SetStatusCondition(&pgdb.Status.Conditions, metav1.Condition{
		Type:    conditionSecretReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Created",
		Message: fmt.Sprintf("Secret %q exists", secretName),
	})

	// 9. Update status
	meta.SetStatusCondition(&pgdb.Status.Conditions, metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Provisioned",
		Message: "Database, user, and secret are ready",
	})
	pgdb.Status.DatabaseName = dbName
	pgdb.Status.Username = userName
	pgdb.Status.SecretName = secretName
	pgdb.Status.ObservedGeneration = pgdb.Generation
	if err := r.Status().Update(ctx, pgdb); err != nil {
		return ctrl.Result{}, err
	}

	r.Recorder.Event(pgdb, corev1.EventTypeNormal, "Provisioned",
		"Database, user, and secret are ready")

	// 10. Requeue for drift detection
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// ensureUser ensures the PostgreSQL user exists and returns the password.
// Returns postgres.ErrNotOwned if the user exists but belongs to a different CR.
func (r *PostgresDatabaseReconciler) ensureUser(
	ctx context.Context,
	pgdb *postgresv1alpha1.PostgresDatabase,
	pgClient postgres.PGClient,
	userName, secretName, provenance string,
) (string, error) {
	exists, err := pgClient.UserExists(ctx, userName)
	if err != nil {
		return "", err
	}

	if exists {
		// Verify provenance — refuse to touch a user we didn't create
		if err := pgClient.CheckUserProvenance(ctx, userName, provenance); err != nil {
			return "", err
		}
	}

	// Try to read existing password from the output secret
	existingSecret := &corev1.Secret{}
	secretExists := false
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: pgdb.Namespace,
	}, existingSecret); err == nil {
		secretExists = true
	}

	if exists && secretExists {
		// User and secret exist — keep current password
		return string(existingSecret.Data["password"]), nil
	}

	// Generate a new password (either user is new or secret was deleted)
	password, err := postgres.GeneratePassword(passwordLen)
	if err != nil {
		return "", fmt.Errorf("generating password: %w", err)
	}

	if !exists {
		if err := pgClient.CreateUser(ctx, userName, password, provenance); err != nil {
			return "", err
		}
	} else {
		// User exists (and provenance matched) but secret is gone — reset password
		if err := pgClient.UpdatePassword(ctx, userName, password); err != nil {
			return "", err
		}
	}

	return password, nil
}

// ensureDatabase ensures the PostgreSQL database exists with correct ownership.
// Returns postgres.ErrNotOwned if the database exists but belongs to a different CR.
func (r *PostgresDatabaseReconciler) ensureDatabase(
	ctx context.Context,
	pgClient postgres.PGClient,
	dbName, owner, provenance string,
) error {
	exists, err := pgClient.DatabaseExists(ctx, dbName)
	if err != nil {
		return err
	}

	if exists {
		// Verify provenance — refuse to touch a database we didn't create
		if err := pgClient.CheckDatabaseProvenance(ctx, dbName, provenance); err != nil {
			return err
		}
		if err := pgClient.EnsureDatabaseOwner(ctx, dbName, owner); err != nil {
			return err
		}
	} else {
		if err := pgClient.CreateDatabase(ctx, dbName, owner, provenance); err != nil {
			return err
		}
	}

	return nil
}

// ensureSecret creates or updates the output Kubernetes secret.
func (r *PostgresDatabaseReconciler) ensureSecret(
	ctx context.Context,
	pgdb *postgresv1alpha1.PostgresDatabase,
	host, port, dbName, userName, password string,
) error {
	secretName := resolvedSecretName(pgdb)
	data := buildSecretData(host, port, dbName, userName, password)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: pgdb.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":               "postgresdb-user-operator",
				"postgres.crds.lunarys.lab/postgresdatabase": pgdb.Name,
			},
		},
		Type: corev1.SecretTypeBasicAuth,
		Data: data,
	}

	if err := controllerutil.SetControllerReference(pgdb, secret, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference: %w", err)
	}

	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: pgdb.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, secret)
	}
	if err != nil {
		return err
	}

	// Update existing secret
	existing.Data = data
	existing.Labels = secret.Labels
	existing.Type = secret.Type
	return r.Update(ctx, existing)
}

// cleanupPostgresResources drops the database and user from PostgreSQL.
func (r *PostgresDatabaseReconciler) cleanupPostgresResources(
	ctx context.Context,
	clusterRef postgresv1alpha1.ClusterReference,
	dbName, userName string,
) error {
	pgClient, _, _, err := r.connectToCluster(ctx, clusterRef)
	if err != nil {
		return fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	defer pgClient.Close(ctx)

	if err := pgClient.DropDatabase(ctx, dbName); err != nil {
		return fmt.Errorf("dropping database %q: %w", dbName, err)
	}

	if err := pgClient.DropUser(ctx, userName); err != nil {
		return fmt.Errorf("dropping user %q: %w", userName, err)
	}

	return nil
}

// resolveClusterRef returns the effective cluster reference for the CR,
// falling back to the operator's default if the CR omits clusterRef.
func (r *PostgresDatabaseReconciler) resolveClusterRef(ctx context.Context, pgdb *postgresv1alpha1.PostgresDatabase) (postgresv1alpha1.ClusterReference, error) {
	if pgdb.Spec.ClusterRef != nil {
		return *pgdb.Spec.ClusterRef, nil
	}
	if r.DefaultClusterSelector != "" {
		return r.findClusterBySelector(ctx)
	}
	if r.DefaultClusterName != "" && r.DefaultClusterNamespace != "" {
		return postgresv1alpha1.ClusterReference{
			Name:      r.DefaultClusterName,
			Namespace: r.DefaultClusterNamespace,
		}, nil
	}
	return postgresv1alpha1.ClusterReference{}, fmt.Errorf(
		"spec.clusterRef is not set and no default cluster is configured (use --default-cluster-name and --default-cluster-namespace, or --default-cluster-selector)")
}

// findClusterBySelector lists CNPG Cluster resources matching DefaultClusterSelector
// and returns the cluster reference if exactly one match is found.
func (r *PostgresDatabaseReconciler) findClusterBySelector(ctx context.Context) (postgresv1alpha1.ClusterReference, error) {
	sel, err := labels.Parse(r.DefaultClusterSelector)
	if err != nil {
		return postgresv1alpha1.ClusterReference{}, fmt.Errorf("invalid --default-cluster-selector %q: %w", r.DefaultClusterSelector, err)
	}

	clusterList := &unstructured.UnstructuredList{}
	clusterList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "postgresql.cnpg.io",
		Version: "v1",
		Kind:    "ClusterList",
	})
	listOpts := []client.ListOption{client.MatchingLabelsSelector{Selector: sel}}
	if r.DefaultClusterNamespace != "" {
		listOpts = append(listOpts, client.InNamespace(r.DefaultClusterNamespace))
	}
	if err := r.List(ctx, clusterList, listOpts...); err != nil {
		return postgresv1alpha1.ClusterReference{}, fmt.Errorf("listing CNPG clusters: %w", err)
	}

	switch len(clusterList.Items) {
	case 0:
		return postgresv1alpha1.ClusterReference{}, fmt.Errorf("no CNPG cluster found matching selector %q", r.DefaultClusterSelector)
	case 1:
		c := clusterList.Items[0]
		return postgresv1alpha1.ClusterReference{Name: c.GetName(), Namespace: c.GetNamespace()}, nil
	default:
		return postgresv1alpha1.ClusterReference{}, fmt.Errorf(
			"selector %q must be unambiguous: found %d CNPG clusters", r.DefaultClusterSelector, len(clusterList.Items))
	}
}

// connectToCluster reads the superuser secret and opens a connection to the PostgreSQL cluster.
func (r *PostgresDatabaseReconciler) connectToCluster(
	ctx context.Context, clusterRef postgresv1alpha1.ClusterReference,
) (postgres.PGClient, string, string, error) {
	suSecretName := clusterRef.Name + "-superuser"
	suSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      suSecretName,
		Namespace: clusterRef.Namespace,
	}, suSecret); err != nil {
		return nil, "", "", err
	}

	suUser := string(suSecret.Data["username"])
	suPass := string(suSecret.Data["password"])
	host := fmt.Sprintf("%s-rw.%s.svc.cluster.local",
		clusterRef.Name, clusterRef.Namespace)
	port := "5432"

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/postgres?sslmode=require",
		url.QueryEscape(suUser), url.QueryEscape(suPass), host, port)

	pgClient, err := r.ConnectPG(ctx, connString)
	if err != nil {
		return nil, "", "", err
	}
	return pgClient, host, port, nil
}

// setNotReadyCondition is a helper to set the Ready condition to False and update status.
func (r *PostgresDatabaseReconciler) setNotReadyCondition(
	ctx context.Context,
	pgdb *postgresv1alpha1.PostgresDatabase,
	reason, message string,
) {
	meta.SetStatusCondition(&pgdb.Status.Conditions, metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	})
	_ = r.Status().Update(ctx, pgdb)
}

// resolvedDatabaseName returns the effective database name for the CR.
func (r *PostgresDatabaseReconciler) resolvedDatabaseName(pgdb *postgresv1alpha1.PostgresDatabase) string {
	name := pgdb.Spec.DatabaseName
	if name == "" {
		name = strings.ReplaceAll(pgdb.Name, "-", "_")
	}
	if r.NamespacePrefix {
		name = strings.ReplaceAll(pgdb.Namespace, "-", "_") + "_" + name
	}
	return name
}

// resolvedUsername returns the effective username for the CR.
func (r *PostgresDatabaseReconciler) resolvedUsername(pgdb *postgresv1alpha1.PostgresDatabase) string {
	if pgdb.Spec.Username != "" {
		name := pgdb.Spec.Username
		if r.NamespacePrefix {
			name = strings.ReplaceAll(pgdb.Namespace, "-", "_") + "_" + name
		}
		return name
	}
	return r.resolvedDatabaseName(pgdb)
}

// resolvedSecretName returns the effective secret name for the CR.
func resolvedSecretName(pgdb *postgresv1alpha1.PostgresDatabase) string {
	if pgdb.Spec.SecretName != "" {
		return pgdb.Spec.SecretName
	}
	return pgdb.Name + "-pgcreds"
}

// buildSecretData constructs the CNPG-compatible secret data map.
func buildSecretData(host, port, dbName, userName, password string) map[string][]byte {
	pgpass := fmt.Sprintf("%s:%s:%s:%s:%s", host, port, dbName, userName, password)
	uri := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s",
		url.QueryEscape(userName), url.QueryEscape(password), host, port, dbName)
	jdbcURI := fmt.Sprintf("jdbc:postgresql://%s:%s/%s?user=%s&password=%s",
		host, port, dbName, url.QueryEscape(userName), url.QueryEscape(password))

	return map[string][]byte{
		"username": []byte(userName),
		"password": []byte(password),
		"host":     []byte(host),
		"port":     []byte(port),
		"dbname":   []byte(dbName),
		"pgpass":   []byte(pgpass),
		"uri":      []byte(uri),
		"jdbc-uri": []byte(jdbcURI),
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresDatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresv1alpha1.PostgresDatabase{}).
		Owns(&corev1.Secret{}).
		Named("postgresdatabase").
		Complete(r)
}
