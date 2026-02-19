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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DeletionPolicy defines what happens to the PostgreSQL database and user when the CR is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type DeletionPolicy string

const (
	DeletionPolicyRetain DeletionPolicy = "Retain"
	DeletionPolicyDelete DeletionPolicy = "Delete"
)

// ClusterReference identifies the CloudNativePG cluster to provision on.
type ClusterReference struct {
	// name is the name of the CNPG Cluster resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// namespace is the namespace where the CNPG Cluster lives.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
}

// PostgresDatabaseSpec defines the desired state of PostgresDatabase.
type PostgresDatabaseSpec struct {
	// clusterRef references the CloudNativePG cluster to provision the database on.
	// If omitted, the operator's default cluster (--default-cluster-name/--default-cluster-namespace) is used.
	// +kubebuilder:validation:Optional
	ClusterRef *ClusterReference `json:"clusterRef,omitempty"`

	// databaseName is the name of the PostgreSQL database to create.
	// Defaults to the CR name with hyphens replaced by underscores.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]*$`
	DatabaseName string `json:"databaseName,omitempty"`

	// username is the PostgreSQL user to create as the database owner.
	// Defaults to the databaseName.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]*$`
	Username string `json:"username,omitempty"`

	// secretName is the name of the Kubernetes Secret to create with connection credentials.
	// Defaults to "<cr-name>-pgcreds".
	// +kubebuilder:validation:Optional
	SecretName string `json:"secretName,omitempty"`

	// deletionPolicy defines what happens to the PostgreSQL database and user
	// when the CR is deleted. "Retain" keeps them (default), "Delete" drops them.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
}

// PostgresDatabaseStatus defines the observed state of PostgresDatabase.
type PostgresDatabaseStatus struct {
	// conditions represent the current state of the PostgresDatabase resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// databaseName is the actual PostgreSQL database name that was provisioned.
	// +optional
	DatabaseName string `json:"databaseName,omitempty"`

	// username is the actual PostgreSQL username that was provisioned.
	// +optional
	Username string `json:"username,omitempty"`

	// secretName is the name of the Secret containing the connection credentials.
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pgdb
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=".status.databaseName"
// +kubebuilder:printcolumn:name="Username",type=string,JSONPath=".status.username"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// PostgresDatabase is the Schema for the postgresdatabases API.
type PostgresDatabase struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresDatabase.
	// +required
	Spec PostgresDatabaseSpec `json:"spec"`

	// status defines the observed state of PostgresDatabase.
	// +optional
	Status PostgresDatabaseStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresDatabaseList contains a list of PostgresDatabase.
type PostgresDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgresDatabase{}, &PostgresDatabaseList{})
}
