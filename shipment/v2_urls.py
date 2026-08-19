"""New Shipment 2.0 routes, mounted at /api/shipment/v2/.

Its own file so 2.0 can be removed by deleting one include() from
shipment/urls.py — nothing in the original planner's URL table is touched.
"""

from django.urls import path

from . import v2_views

urlpatterns = [
    path('channels/', v2_views.V2ChannelsView.as_view(), name='v2-channels'),
    path('appointments/', v2_views.V2AppointmentsView.as_view(), name='v2-appointments'),
    # Line-by-line eligibility for ONE appointment, fetched when the card's
    # detail link is opened rather than for every card in the list.
    path('appointments/<str:appointment_id>/lines/',
         v2_views.V2AppointmentDetailView.as_view(), name='v2-appointment-lines'),
    path('pos/', v2_views.V2PoBookView.as_view(), name='v2-po-book'),
    # More specific route first: 'fill/options/' must not be swallowed by
    # 'fill/'. They are different methods (GET vs POST) so a collision would not
    # even 405 — it would 404 on a URL that plainly exists.
    path('fill/options/', v2_views.V2FillOptionsView.as_view(), name='v2-fill-options'),
    path('fill/', v2_views.V2FillView.as_view(), name='v2-fill'),
]
