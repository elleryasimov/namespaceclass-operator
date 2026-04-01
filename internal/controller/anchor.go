package controller

import (
	"context"

	namespaceclassv1alpha1 "github.com/elleryasimov/namespaceclass-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Currently we use a config-map as the anchor
const AnchorName = "namespaceclass-anchor"

func (r *NamespaceClassReconciler) getAnchor(ctx context.Context, nsName string) (client.Object, error) {
	anchor := &corev1.ConfigMap{}

	key := types.NamespacedName{
		Name:      AnchorName,
		Namespace: nsName,
	}

	err := r.Get(ctx, key, anchor)

	return anchor, err
}

func (r *NamespaceClassReconciler) createAnchor(ctx context.Context, nsName string, nsClass *namespaceclassv1alpha1.NamespaceClass) (client.Object, error) {
	anchor := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AnchorName,
			Namespace: nsName,
		},
	}
	controllerutil.SetControllerReference(nsClass, anchor, r.Scheme)
	err := r.Create(ctx, anchor)

	return anchor, err
}

func (r *NamespaceClassReconciler) deleteAnchor(ctx context.Context, anchor client.Object) error {
	return r.Delete(ctx, anchor)
}
