from xml.etree import ElementTree as ET
import neo4j.v1 as neo

if __name__ == "__main__":
    # The main module must import files from the same directory in this way, but PyCharm just can't recognize it.
    # http://stackoverflow.com/questions/41816973/modulenotfounderror-what-does-it-mean-main-is-not-a-package
    from src.CypherStatementBuilder import CypherStatementBuilder as Stmt
    from src.Entities import *
else:
    # So do a trick, use the standard Python 3 import syntax to feed PyCharm's intellisense.
    from .CypherStatementBuilder import CypherStatementBuilder as Stmt
    from .Entities import *


class SessionExtension:
    _session: neo.Session = None
    _transaction: neo.Transaction = None
    _known_org_refs: List[str] = []
    _known_orgs: List[Organization] = []
    _added_org_refs: List[str] = []
    _known_location_codes: List[str] = []
    _known_locations: List[Location] = []
    _added_location_codes: List[str] = []
    _known_policy_codes: List[int] = []
    _known_policies: List[Policy] = []
    _added_policy_codes: List[int] = []

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
        code: int = int(node.get("code"))
        name: str = SessionExtension.narrative(node)
        if code in self._added_policy_codes:
            index = self._added_policy_codes.index(code)
            pol: Policy = self._known_policies[index]
            return pol
        vocabulary: int = int(node.get("vocabulary"))
        policy = Policy(name, vocabulary, code)
        self._known_policy_codes.append(policy.code)
        self._known_policies.append(policy)
        return policy

    def add_policy(self, policy: Policy) -> int:
        if policy.code in self._added_policy_codes:
            index = self._known_policy_codes.index(policy.code)
            pol: Policy = self._known_policies[index]
            return pol.obj_id
        self._added_policy_codes.append(policy.code)
        stmt = Stmt.create_node(policy.get_name(), "Policy", {
            "name": policy.name, "vocabulary": policy.vocabulary, "code": policy.code,
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
        name = SessionExtension.narrative(node)
        location = Location(code, name)
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
