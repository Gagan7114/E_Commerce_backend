from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from accounts.permissions import require

from . import service


def _page(request) -> tuple[int, int]:
    try:
        page = max(1, int(request.query_params.get("page", 1)))
        page_size = min(200, max(1, int(request.query_params.get("page_size", 50))))
    except ValueError:
        page, page_size = 1, 50
    return page, page_size


@api_view(["GET"])
@permission_classes([require("sap.view")])
def sap_distributors(request):
    search = request.query_params.get("search", "").strip()
    page, page_size = _page(request)
    rows, total = service.distributors(search, limit=page_size, offset=(page - 1) * page_size)
    return Response({"count": total, "page": page, "page_size": page_size, "results": rows})


@api_view(["GET"])
@permission_classes([require("sap.invoice.view")])
def sap_invoices(request):
    search = request.query_params.get("search", "").strip()
    page, page_size = _page(request)
    rows, total = service.invoices(search, limit=page_size, offset=(page - 1) * page_size)
    return Response({"count": total, "page": page, "page_size": page_size, "results": rows})
