from __future__ import annotations

from pathlib import Path

from django.conf import settings


_FIREBASE_READY = False


def _initialize_firebase():
    global _FIREBASE_READY
    if _FIREBASE_READY:
        return True, "initialized"
    if not getattr(settings, "FIREBASE_NOTIFICATIONS_ENABLED", False):
        return False, "disabled"
    credentials_file = getattr(settings, "FIREBASE_CREDENTIALS_FILE", "")
    if not credentials_file:
        return False, "missing FIREBASE_CREDENTIALS_FILE"
    credentials_path = Path(credentials_file)
    if not credentials_path.exists():
        return False, "FIREBASE_CREDENTIALS_FILE not found"
    try:
        import firebase_admin
        from firebase_admin import credentials
    except ImportError:
        return False, "firebase-admin is not installed"

    if not firebase_admin._apps:
        cred = credentials.Certificate(str(credentials_path))
        firebase_admin.initialize_app(cred)
    _FIREBASE_READY = True
    return True, "initialized"


def send_inventory_doh_notification(notification) -> dict:
    ready, reason = _initialize_firebase()
    if not ready:
        return {"sent": False, "reason": reason}
    try:
        from firebase_admin import messaging
    except ImportError:
        return {"sent": False, "reason": "firebase-admin is not installed"}

    topic = getattr(settings, "FIREBASE_DOH_TOPIC", "inventory_doh_alerts")
    message = messaging.Message(
        notification=messaging.Notification(
            title=notification.title,
            body=notification.message,
        ),
        data={
            "id": str(notification.id),
            "type": notification.alert_type,
            "format": notification.format,
            "platform_slug": notification.platform_slug,
            "sku_code": notification.sku_code,
            "doh": str(notification.doh),
            "link": f"/notifications/inventory-doh/{notification.id}",
        },
        topic=topic,
    )
    response = messaging.send(message)
    return {"sent": True, "message_id": response, "topic": topic}
