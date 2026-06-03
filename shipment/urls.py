from django.urls import path
from . import views

urlpatterns = [
    path('appointments/dates/', views.AppointmentDatesView.as_view(), name='appointment-dates'),
    path('appointments/', views.AppointmentListView.as_view(), name='appointment-list'),
    path('appointments/<str:appointment_id>/items/', views.AppointmentItemsView.as_view(), name='appointment-items'),
    path('appointments/<str:appointment_id>/extra-pos/', views.AppointmentExtraPosView.as_view(), name='appointment-extra-pos'),
    path('po-items/', views.POListView.as_view(), name='po-items'),
    path('asin-catalog/', views.AsinCatalogView.as_view(), name='asin-catalog'),
    path('all-appointments/', views.AllAppointmentsView.as_view(), name='all-appointments'),
    path('appointment-commits/import/', views.AppointmentCommitImportView.as_view(), name='appointment-commits-import'),
    path('shipments/', views.ShipmentListCreateView.as_view(), name='shipment-list-create'),
    path('shipments/stats/', views.ShipmentStatsView.as_view(), name='shipment-stats'),
    path('shipments/pending-approvals/', views.ShipmentPendingApprovalsView.as_view(), name='shipment-pending-approvals'),
    path('shipments/manual-plan/', views.ManualPlanView.as_view(), name='shipment-manual-plan'),
    path('shipments/doh-auto-fill/', views.DOHAutoFillView.as_view(), name='shipment-doh-auto-fill'),
    path('po-shipment-lookup/', views.PoShipmentLookupView.as_view(), name='po-shipment-lookup'),
    path('po-short-supply/', views.PoShortSupplyView.as_view(), name='po-short-supply'),
    path('shipments/<int:pk>/', views.ShipmentDetailView.as_view(), name='shipment-detail'),
    path('shipments/<int:pk>/items/<int:item_id>/', views.ShipmentItemUpdateView.as_view(), name='shipment-item-update'),
    path('shipments/<int:pk>/submit/', views.ShipmentSubmitView.as_view(), name='shipment-submit'),
    path('shipments/<int:pk>/approve/', views.ShipmentApproveView.as_view(), name='shipment-approve'),
    path('shipments/<int:pk>/reject/', views.ShipmentRejectView.as_view(), name='shipment-reject'),
    path('shipments/<int:pk>/dispatch/', views.ShipmentDispatchView.as_view(), name='shipment-dispatch'),
]
