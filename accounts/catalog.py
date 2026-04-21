"""Central catalog of ECMS custom permission codes and default groups.

This module is the single source of truth for:
  - The 25 custom permission codes the app recognizes.
  - The default group -> permission assignments.

Both seed_permissions and seed_groups read from here, and the data migrations
fall back to the same values so a fresh `migrate` gives you a usable system.
"""

PERMISSION_CATALOG: tuple[tuple[str, str], ...] = (
    # Dashboard
    ("dashboard.view", "View dashboard"),
    ("dashboard.table.view", "View dashboard table counts"),
    ("dashboard.inspect", "Inspect dashboard tables"),
    # Platform
    ("platform.view", "View platform pages"),
    ("platform.stats.view", "View platform stats"),
    ("platform.po.view", "View platform purchase orders"),
    ("platform.inventory.view", "View platform inventory"),
    ("platform.secondary.view", "View platform secondary sales"),
    # Dispatch
    ("dispatch.view", "View dispatches"),
    ("dispatch.add", "Create dispatches"),
    ("dispatch.edit", "Edit dispatches"),
    ("dispatch.delete", "Delete dispatches"),
    # Distributor / SAP
    ("distributor.view", "View distributors"),
    ("sap.view", "Query SAP HANA"),
    ("sap.invoice.view", "View SAP invoices"),
    # Admin
    ("admin.access", "Access admin panel"),
    ("admin.user.view", "View users in admin"),
    ("admin.user.manage", "Manage users in admin"),
    ("admin.group.manage", "Manage groups in admin"),
    ("admin.dispatch.manage", "Manage dispatches in admin"),
    ("admin.platform.manage", "Manage platforms in admin"),
    ("admin.warehouse.view", "View warehouse tables in admin"),
    # Uploads
    ("upload.use", "Use the bulk upload tool"),
)

GROUP_CATALOG: dict[str, list[str]] = {
    "Super Admin": ["*"],  # expanded to all codes at seed time
    "Platform Admin": [
        "dashboard.view", "dashboard.table.view",
        "platform.view", "platform.stats.view", "platform.po.view",
        "platform.inventory.view", "platform.secondary.view",
        "dispatch.view", "dispatch.add", "dispatch.edit",
        "admin.access", "admin.platform.manage", "admin.dispatch.manage",
    ],
    "Operations Manager": [
        "dashboard.view", "dashboard.table.view", "dashboard.inspect",
        "platform.view", "platform.stats.view", "platform.po.view",
        "platform.inventory.view", "platform.secondary.view",
        "dispatch.view", "dispatch.add", "dispatch.edit",
        "distributor.view",
    ],
    "Dispatch Operator": [
        "platform.view", "platform.po.view",
        "dispatch.view", "dispatch.add", "dispatch.edit",
    ],
    "Finance Analyst": [
        "dashboard.view", "dashboard.table.view",
        "platform.view", "platform.stats.view", "platform.secondary.view",
        "distributor.view", "sap.view", "sap.invoice.view",
    ],
    "Viewer": [
        "dashboard.view", "platform.view", "platform.stats.view",
        "dispatch.view", "distributor.view",
    ],
    "Uploader": [
        "upload.use",
    ],
}
