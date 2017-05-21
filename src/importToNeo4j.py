from typing import Union
from xml.etree import ElementTree as ET
import re
import neo4j.v1 as neo
from neo4j.v1 import GraphDatabase, basic_auth

if __name__ == "__main__":
    # The main module must import files from the same directory in this way, but PyCharm just can't recognize it.
    # http://stackoverflow.com/questions/41816973/modulenotfounderror-what-does-it-mean-main-is-not-a-package
    from CypherStatementBuilder import CypherStatementBuilder as CSB
    from Entities import *
else:
    # So do a trick, use the standard Python 3 import syntax to feed PyCharm's intellisense.
    from .CypherStatementBuilder import CypherStatementBuilder as CSB
    from .Entities import *

SERVER_URL = "localhost:7687"
AUTH_USER = "neo4j"
AUTH_PASSWORD = "neo"


class SessionExtension:
    _session: Union[neo.Session, neo.Transaction] = None
    _known_org_refs: list = []
    _known_orgs: list = []
    _added_org_refs: list = []
    _known_location_codes: list = []
    _known_locations: list = []
    _added_location_codes: list = []

    def __init__(self, session: Union[neo.Session, neo.Transaction]):
        self._session = session

    def _get_narrative(self, node: ET.Element) -> str:
        return node.find("narrative").text

    def clear_db(self) -> None:
        self._session.run("match(n) detach delete n;")

    def get_activity(self, node: ET.Element) -> Activity:
        ident_node: ET.Element = node.find("iati-identifier")
        identifier: str = ident_node.text
        desc_node: ET.Element = node.find("description")
        description: str = self._get_narrative(desc_node)
        status_node: ET.Element = node.find("activity-status")
        status: int = int(status_node.get("code"))
        return Activity(identifier, description, status)

    def add_activity(self, activity: Activity) -> int:
        stmt = CSB.create_node("act_" + activity.identifier.replace("-", "_"), "Activity", {
            "identifier": activity.identifier, "description": activity.description, "status": activity.status,
            "obj_id": activity.obj_id
        })
        self._session.run(stmt)
        return activity.obj_id

    def get_budget(self, node: ET.Element) -> Budget:
        period_start: str = node.find("period-start").get("iso-date")
        period_end: str = node.find("period-end").get("iso-date")
        value_node: ET.Element = node.find("value")
        value: int = int(value_node.text)
        value_date: str = value_node.get("value-date")
        return Budget(period_start, period_end, value, value_date)

    def add_budget(self, budget: Budget, parent_activity: Activity) -> int:
        # Budget naming: bud_{$activity_ident}
        stmt = CSB.create_node("bud_" + parent_activity.identifier.replace("-", "_"), "Budget", {
            "period_start": budget.period_start, "period_end": budget.period_end,
            "value": budget.value, "value_date": budget.value_date,
            "obj_id": budget.obj_id
        })
        self._session.run(stmt)
        return budget.obj_id

    def get_disbursement(self, node: ET.Element) -> Disbursement:
        period_start: str = node.find("period-start").get("iso-date")
        period_end: str = node.find("period-end").get("iso-date")
        value_node: ET.Element = node.find("value")
        value: int = int(value_node.text)
        value_date: str = value_node.get("value-date")
        return Disbursement(period_start, period_end, value, value_date)

    def add_disbursement(self, disbursement: Disbursement, parent_activity: Activity, index: int) -> int:
        # Disbursement naming: dis_{$activity_ident}_{$index}
        stmt = CSB.create_node("dis_" + parent_activity.identifier.replace("-", "_") + "_{}".format(index),
                               "Disbursement", {
                                   "period_start": disbursement.period_start, "period_end": disbursement.period_end,
                                   "value": disbursement.value, "value_date": disbursement.value_date,
                                   "obj_id": disbursement.obj_id
                               })
        self._session.run(stmt)
        return disbursement.obj_id

    def get_organization(self, node: ET.Element) -> Organization:
        name: str = self._get_narrative(node)
        ref: str = node.get("ref")
        ty: int = int(node.get("type"))
        # Some organizations, like Steps Towards Development, do not have a ref.
        ref = re.sub(r"[.()\[\]' \-*,/&]", "_", name) if ref is None else ref
        if re.match(r"^[^A-Za-z_].*", ref):
            ref = "Org_" + ref
        if ref in self._added_org_refs:
            index = self._known_org_refs.index(ref)
            org: Organization = self._known_orgs[index]
            return org
        organization = Organization(name, ref, ty)
        self._known_org_refs.append(organization.ref)
        self._known_orgs.append(organization)
        return organization

    def add_organization(self, organization: Organization) -> int:
        if organization.ref in self._added_org_refs:
            index = self._known_org_refs.index(organization.ref)
            org: Organization = self._known_orgs[index]
            return org.obj_id
        self._added_org_refs.append(organization.ref)
        stmt = CSB.create_node(organization.ref.replace("-", "_"), "Organization", {
            "name": organization.name, "ref": organization.ref, "type": organization.type,
            "obj_id": organization.obj_id
        })
        self._session.run(stmt)
        return organization.obj_id

    def get_policy(self, node: ET.Element) -> Policy:
        name: str = self._get_narrative(node)
        vocabulary: int = int(node.get("vocabulary"))
        code: int = int(node.get("code"))
        significance: int = int(node.get("significance"))
        return Policy(name, vocabulary, code, significance)

    def add_policy(self, policy: Policy) -> int:
        stmt = CSB.create_node("", "Policy", {
            "name": policy.name, "vocabulary": policy.vocabulary, "code": policy.code,
            "significance": policy.significance,
            "obj_id": policy.obj_id
        })
        self._session.run(stmt)
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
        stmt = CSB.create_node(location.name, "Location", {
            "code": location.code, "name": location.name,
            "obj_id": location.obj_id
        })
        self._session.run(stmt)
        return location.obj_id


def main():
    driver: neo.Driver = GraphDatabase.driver("bolt://" + SERVER_URL, auth=basic_auth(AUTH_USER, AUTH_PASSWORD))
    session: neo.Session = driver.session()
    trans: neo.Transaction = session.begin_transaction()

    tree = ET.ElementTree(file="../data/IATIACTIVITIES19972007.xml")

    ext = SessionExtension(trans)

    print("Clearing database...")
    ext.clear_db()

    print("Adding activity info...")
    for activity_node in tree.iter("iati-activity"):
        activity_node: ET.Element = activity_node
        # Add an activity
        activity = ext.get_activity(activity_node)
        ext.add_activity(activity)
        # Add its budget
        budget_node: ET.Element = activity_node.find("budget")
        budget = ext.get_budget(budget_node)
        ext.add_budget(budget, activity)
        # Add planned disbursements
        disbursement_index = 0
        for disbursement_node in activity_node.iter("planned-disbursement"):
            disbursement = ext.get_disbursement(disbursement_node)
            ext.add_disbursement(disbursement, activity, disbursement_index)
            disbursement_index += 1
        # Organizations
        reporting_org_node: ET.Element = activity_node.find("reporting-org")
        organization = ext.get_organization(reporting_org_node)
        ext.add_organization(organization)
        for participating_org_node in activity_node.iter("participating-org"):
            organization = ext.get_organization(participating_org_node)
            ext.add_organization(organization)
        # Policy markers
        for policy_marker_node in activity_node.iter("policy-marker"):
            policy = ext.get_policy(policy_marker_node)
            ext.add_policy(policy)
        # Locations
        recipient_node = activity_node.find("recipient-country")
        if recipient_node is None:
            recipient_node = activity_node.find("recipient-region")
        location = ext.get_location(recipient_node)
        ext.add_location(location)

    trans.commit()

    session.close()


if __name__ == '__main__':
    main()
