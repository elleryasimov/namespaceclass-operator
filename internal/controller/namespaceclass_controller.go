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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	namespaceclassv1alpha1 "github.com/elleryasimov/namespaceclass-operator/api/v1alpha1"
)

type NsClassObjInfo struct {
	ObjName string
	ObjKind string
}

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
	// 1. Get the namespace.
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, req.NamespacedName, ns); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Get the namespace class bind to namespace.
	nsClassExisted := false
	nsClassName := ns.Labels["namespaceclass.akuity.io/name"]
	nsClass := &namespaceclassv1alpha1.NamespaceClass{}
	if nsClassName != "" {
		if err := r.Get(ctx, types.NamespacedName{Name: nsClassName}, nsClass); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		} else {
			nsClassExisted = true
		}
	}

	// 3. Get the anchor inside the namespace.
	anchorExisted := true
	anchor, err := r.getAnchor(ctx, ns.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			anchorExisted = false
		} else {
			return ctrl.Result{}, err
		}
	}

	// 4. If namespace class changed, delete the anchor and create a new one.
	//    The new anchor's controller reference should be set to the current namespace class in the namespace.
	if anchorExisted && nsClassExisted {
		anchorController := metav1.GetControllerOf(anchor)
		if anchorController != nil && anchorController.UID != nsClass.UID {
			err := r.deleteAnchor(ctx, anchor)
			if err != nil {
				// Setup condition, retry
				return ctrl.Result{}, err
			}
			anchor, err = r.createAnchor(ctx, ns.Name, nsClass)
			if err != nil {
				// Setup condition, retry
				return ctrl.Result{}, err
			}
		}
	} else if !anchorExisted && nsClassExisted {
		anchor, err = r.createAnchor(ctx, ns.Name, nsClass)
		if err != nil {
			// Setup condition, retry
			return ctrl.Result{}, err
		}
	} else if anchorExisted && !nsClassExisted {
		err := r.deleteAnchor(ctx, anchor)
		if err != nil {
			// Setup condition, retry
			return ctrl.Result{}, err
		}
	}

	// 5. Apply the resources defined in the namespace class. Set their controller reference to the anchor.
	for _, raw := range nsClass.Spec.Resources {
		obj := &unstructured.Unstructured{}
		_ = obj.UnmarshalJSON(raw.Raw) // Simplified for brevity

		obj.SetNamespace(ns.Name)
		obj.SetLabels(map[string]string{"namespaceclass.akuity.io/name": nsClassName})
		controllerutil.SetControllerReference(anchor, obj, r.Scheme)

		// Use Server-Side Apply to "Force" the template's state
		err := r.Patch(ctx, obj, client.Apply, client.FieldOwner("template-controller"), client.ForceOwnership)
		if err != nil {
			// Setup conditions
			return ctrl.Result{}, err
		}

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
