"""Read/write helpers for the global feature switches (accounts.FeatureFlag).

Two switches, both owned by the account holding `profile_controls.manage`:

    uploader   — OFF stops everyone uploading through the Uploaders UI. CLI and
                 other automated clients are NEVER affected; see
                 is_browser_request() below.
    game_play  — OFF stops everyone except the owner playing the JivoBot game.

Every read fails OPEN (returns enabled). A missing row, a fresh database or a
database error must not silently lock a feature out.
"""

from django.db import DatabaseError

from .models import FeatureFlag


def flags_state() -> dict[str, bool]:
    """Current state of every known switch, defaulting missing rows to True."""
    state = {key: True for key in FeatureFlag.KEYS}
    try:
        rows = FeatureFlag.objects.filter(key__in=FeatureFlag.KEYS).values_list(
            "key", "is_enabled"
        )
        for key, is_enabled in rows:
            state[key] = bool(is_enabled)
    except DatabaseError:
        # Fail open — an unreachable table must not block uploads or the game.
        pass
    return state


def flag_enabled(key: str) -> bool:
    """Is one switch on? Unknown keys and read failures count as on."""
    try:
        row = FeatureFlag.objects.filter(key=key).values_list("is_enabled", flat=True)
        return bool(row[0]) if row else True
    except DatabaseError:
        return True


def set_flag(key: str, enabled: bool, user=None) -> bool:
    """Flip one switch, recording who did it. Returns the stored value."""
    if key not in FeatureFlag.KEYS:
        raise ValueError(f"Unknown feature flag: {key}")
    FeatureFlag.objects.update_or_create(
        key=key,
        defaults={
            "is_enabled": bool(enabled),
            "updated_by": user if getattr(user, "pk", None) else None,
        },
    )
    return bool(enabled)


def is_browser_request(request) -> bool:
    """True when this request came from a web browser, not a CLI or a script.

    The uploader switch must never stop the automation, so enforcement asks
    "is this definitely a browser?" and defaults to NO. An unrecognised client
    (a new CLI, a cron script, Apps Script) is therefore always allowed through.

    `Origin` is the reliable signal: browsers attach it to every request whose
    method isn't GET/HEAD and — being a forbidden header — page JavaScript can
    neither remove nor forge it, so a signed-in user cannot bypass the switch
    from devtools. `Referer` and a `Mozilla/*` User-Agent are backstops for a
    non-XHR form post. Go's net/http (the upload CLI) sends none of the three.
    """
    meta = request.META
    if meta.get("HTTP_ORIGIN") or meta.get("HTTP_REFERER"):
        return True
    return str(meta.get("HTTP_USER_AGENT", "")).startswith("Mozilla/")
