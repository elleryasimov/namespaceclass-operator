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

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	namespaceclassv1alpha1 "github.com/elleryasimov/namespaceclass-operator/api/v1alpha1"
)

const policyName = "namespaceclass-networkpolicy"

// NamespaceClassReconciler reconciles a NamespaceClass object
type NamespaceClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=namespaceclass.akuity.io,resources=namespaceclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=namespaceclass.akuity.io,resources=namespaceclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=namespaceclass.akuity.io,resources=namespaceclasses/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NamespaceClass object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *NamespaceClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	var ns corev1.Namespace
	if err := r.Get(ctx, req.NamespacedName, &ns); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	namespaceClassName, size := ns.Labels["namespaceclass.akuity.io/name"]
	// should deal with clear logic
	if !size || namespaceClassName == "" {
		var networkPolicy networkingv1.NetworkPolicy
		err := r.Get(ctx, types.NamespacedName{Name: policyName, Namespace: ns.Name}, &networkPolicy)

		if err == nil {
			if err := r.Delete(ctx, &networkPolicy); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("Prune networkpolicy because namespaceclass label was removed", "ns", ns.Name)
		}
		return ctrl.Result{}, nil
	}

	var namespaceClass namespaceclassv1alpha1.NamespaceClass
	if err := r.Get(ctx, types.NamespacedName{Name: namespaceClassName}, &namespaceClass); err != nil {
		logger.Error(err, "Failed to find referenced namespaceclass", "namespaceclass", namespaceClassName)
		return ctrl.Result{}, err
	}

	if err := r.syncNetworkPolicy(ctx, &ns, &namespaceClass); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}).
		Named("namespaceclass").
		Watches(
			&namespaceclassv1alpha1.NamespaceClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapProfileToNamespaces),
		).
		Owns(&networkingv1.NetworkPolicy{}).
		Complete(r)
}

func (r *NamespaceClassReconciler) mapProfileToNamespaces(ctx context.Context, obj client.Object) []reconcile.Request {
	namespaceClassName := obj.GetName()
	var nsList corev1.NamespaceList

	selector := labels.SelectorFromSet(labels.Set{"namespaceclass.akuity.io/name": namespaceClassName})
	if err := r.List(ctx, &nsList, &client.ListOptions{LabelSelector: selector}); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, len(nsList.Items))
	for i, ns := range nsList.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name: ns.Name,
			},
		}
	}
	return requests
}

func (r *NamespaceClassReconciler) syncNetworkPolicy(ctx context.Context, namespace *corev1.Namespace, namespaceclass *namespaceclassv1alpha1.NamespaceClass) error {
	networkPolicy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      policyName,
			Namespace: namespace.Name,
		},
	}

	// CreateOrUpdate will:
	// 1. Check if it exists
	// 2. If not, call the function to set it up and Create
	// 3. If yes, call the function to update fields and Update
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, networkPolicy, func() error {
		networkPolicy.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{},
			Ingress:     namespaceclass.Spec.IngressRules,
			Egress:      namespaceclass.Spec.EgressRules,
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		}

		// IMPORTANT: Set the Namespace as the owner
		return controllerutil.SetControllerReference(namespace, networkPolicy, r.Scheme)
	})
	return err
}
