class SAPReadOnlyRouter:
    """Guards against accidental writes to a `sap` DB alias.

    We don't actually register a `sap` Django DB connection — HANA is accessed
    via hdbcli in sap/service.py. This router is defense in depth: if anyone
    later adds a `sap` entry to DATABASES, the router blocks writes and
    migrations against it.
    """

    sap_alias = "sap"

    def db_for_read(self, model, **hints):
        return None

    def db_for_write(self, model, **hints):
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == self.sap_alias:
            return False
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return None
