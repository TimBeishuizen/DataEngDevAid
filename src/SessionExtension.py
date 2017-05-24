from xml.etree import ElementTree as ET
import neo4j.v1 as neo

if __name__ == "__main__":
    # The main module must import files from the same directory in this way, but PyCharm just can't recognize it.
    # http://stackoverflow.com/questions/41816973/modulenotfounderror-what-does-it-mean-main-is-not-a-package
    from CypherStatementBuilder import CypherStatementBuilder as Stmt
    from Entities import *
else:
    # So do a trick, use the standard Python 3 import syntax to feed PyCharm's intellisense.
    from .CypherStatementBuilder import CypherStatementBuilder as Stmt
    from .Entities import *


class SessionExtension:
    _session: neo.Session = None
    _transaction: neo.Transaction = None
    _known_org_refs: list = []
    _known_orgs: list = []
    _added_org_refs: list = []
    _known_location_codes: list = []
    _known_locations: list = []
    _added_location_codes: list = []

    def __init__(self, session: neo.Session):
        self._session = session

    @staticmethod
    def narrative(node: ET.Element) -> str:
        return node.find("narrative").text

    def begin_transaction(self) -> None:
        if self._transaction is not None:
            return
        self._transaction = self._session.begin_transaction()

    def commit(self) -> None:
        self._transaction.commit()
        self._transaction.close()
        self._transaction = None

    def rollback(self) -> None:
        self._transaction.rollback()
        self._transaction.close()
        self._transaction = None

    def run(self, query: str) -> None:
        self._transaction.run(query)

    def get_activity(self, node: ET.Element) -> Activity:
        ident_node: ET.Element = node.find("iati-identifier")
        identifier: str = ident_node.text
        desc_node: ET.Element = node.find("description")
        description: str = SessionExtension.narrative(desc_node)
        status_node: ET.Element = node.find("activity-status")
        status: int = int(status_node.get("code"))
        dates: List[Activity.ActivityDate] = []
        for act_date_node in node.iter("activity-date"):
            dates.append(Activity.ActivityDate(int(act_date_node.get("type")), act_date_node.get("iso-date")))
        return Activity(identifier, description, status, dates)

    def add_activity(self, activity: Activity) -> int:
        stmt = Stmt.create_node(activity.get_name(), "Activity", {
            "identifier": activity.identifier, "description": activity.description, "status": activity.status,
            "obj_id": activity.obj_id
        })
        self._transaction.run(stmt)
        return activity.obj_id

    def get_budget(self, node: ET.Element, parent_activity: Activity) -> Budget:
        period_start: str = node.find("period-start").get("iso-date")
        period_end: str = node.find("period-end").get("iso-date")
        value_node: ET.Element = node.find("value")
        value: int = int(value_node.text)
        value_date: str = value_node.get("value-date")
        return Budget(period_start, period_end, value, value_date, parent_activity)

    def add_budget(self, budget: Budget) -> int:
        # Budget naming: bud_{$activity_ident}
        stmt = Stmt.create_node(budget.get_name(), "Budget", {
            "period_start": budget.period_start, "period_end": budget.period_end,
            "value": budget.value, "value_date": budget.value_date,
            "obj_id": budget.obj_id
        })
        self._transaction.run(stmt)
        return budget.obj_id

    def get_disbursement(self, node: ET.Element, parent_activity: Activity, index: int) -> Disbursement:
        period_start: str = node.find("period-start").get("iso-date")
        period_end: str = node.find("period-end").get("iso-date")
        value_node: ET.Element = node.find("value")
        value: int = int(value_node.text)
        value_date: str = value_node.get("value-date")
        return Disbursement(period_start, period_end, value, value_date, parent_activity, index)

    def add_disbursement(self, disbursement: Disbursement) -> int:
        # Disbursement naming: dis_{$activity_ident}_{$index}
        stmt = Stmt.create_node(disbursement.get_name(), "Disbursement", {
            "period_start": disbursement.period_start, "period_end": disbursement.period_end,
            "value": disbursement.value, "value_date": disbursement.value_date,
            "obj_id": disbursement.obj_id
        })
        self._transaction.run(stmt)
        return disbursement.obj_id

    def get_organization(self, node: ET.Element) -> Organization:
        name: str = SessionExtension.narrative(node)
        ref: str = node.get("ref")
        ty: int = int(node.get("type"))
        ref = Organization.get_unique_ref(name, ref)
        if ref in self._added_org_refs:
            index = self._known_org_refs.index(ref)
            org: Organization = self._known_orgs[index]
            return org
        organization = Organization(name, ref, ty)
        self._known_org_refs.append(organization.ref)
        self._known_orgs.append(organization)
        return organization

    def add_organization(self, org: Organization) -> int:
        if org.ref in self._added_org_refs:
            index = self._known_org_refs.index(org.ref)
            org: Organization = self._known_orgs[index]
            return org.obj_id
        self._added_org_refs.append(org.ref)
        stmt = Stmt.create_node(org.get_name(), "Organization", {
            "name": org.name, "ref": org.ref, "type": org.type,
            "obj_id": org.obj_id
        })
        self._transaction.run(stmt)
        return org.obj_id

    def get_policy(self, node: ET.Element) -> Policy:
        name: str = SessionExtension.narrative(node)
        vocabulary: int = int(node.get("vocabulary"))
        code: int = int(node.get("code"))
        significance: int = int(node.get("significance"))
        return Policy(name, vocabulary, code, significance)

    def add_policy(self, policy: Policy) -> int:
        stmt = Stmt.create_node(policy.get_name(), "Policy", {
            "name": policy.name, "vocabulary": policy.vocabulary, "code": policy.code,
            "significance": policy.significance,
            "obj_id": policy.obj_id
        })
        self._transaction.run(stmt)
        return policy.obj_id

    def get_location(self, node: ET.Element) -> Location:
        code: str = node.get("code")
        if code in self._added_location_codes:
            index = self._known_location_codes.index(code)
            loc: Location = self._known_locations[index]
            return loc
        location = Location(code)
        self._known_location_codes.append(location.code)
        self._known_locations.append(location)
        return location

    def add_location(self, location: Location) -> int:
        if location.code in self._added_location_codes:
            index = self._known_location_codes.index(location.code)
            loc: Location = self._known_locations[index]
            return loc.obj_id
        self._added_location_codes.append(location.code)
        stmt = Stmt.create_node(location.get_name(), "Location", {
            "code": location.code, "name": location.name,
            "obj_id": location.obj_id
        })
        self._transaction.run(stmt)
        return location.obj_id
