from typing import Dict, Any
from xml.etree import ElementTree as ET
from time import localtime, strftime
import neo4j.v1 as neo
import neo4j.exceptions as neo_ex
from neo4j.v1 import GraphDatabase, basic_auth

if __name__ == "__main__":
    # The main module must import files from the same directory in this way, but PyCharm just can't recognize it.
    # http://stackoverflow.com/questions/41816973/modulenotfounderror-what-does-it-mean-main-is-not-a-package
    from CypherStatementBuilder import CypherStatementBuilder as Stat
    from Entities import *
else:
    # So do a trick, use the standard Python 3 import syntax to feed PyCharm's intellisense.
    from .CypherStatementBuilder import CypherStatementBuilder as Stat
    from .Entities import *

SERVER_URL = "localhost:7687"
AUTH_USER = "neo4j"
AUTH_PASSWORD = "neo"

CLASS_LIST = ["Activity", "Budget", "Disbursement", "Organization", "Policy", "Location"]
XML_FILES = [
    "../data/IATIACTIVITIES19972007.xml",
    "../data/IATIACTIVITIES20082009.xml",
    "../data/IATIACTIVITIES20102011.xml",
    "../data/IATIACTIVITIES20122013.xml",
    "../data/IATIACTIVITIES20142015.xml",
    "../data/IATIACTIVITIES20162017.xml"
]


def timestr() -> str:
    return strftime("%Y-%m-%d %H:%M:%S", localtime())


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

    def _get_narrative(self, node: ET.Element) -> str:
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
        description: str = self._get_narrative(desc_node)
        status_node: ET.Element = node.find("activity-status")
        status: int = int(status_node.get("code"))
        dates: List[Activity.ActivityDate] = []
        for act_date_node in node.iter("activity-date"):
            dates.append(Activity.ActivityDate(int(act_date_node.get("type")), act_date_node.get("iso-date")))
        return Activity(identifier, description, status, dates)

    def add_activity(self, activity: Activity) -> int:
        stmt = Stat.create_node(activity.get_name(), "Activity", {
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
        stmt = Stat.create_node(budget.get_name(), "Budget", {
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
        stmt = Stat.create_node(disbursement.get_name(), "Disbursement", {
            "period_start": disbursement.period_start, "period_end": disbursement.period_end,
            "value": disbursement.value, "value_date": disbursement.value_date,
            "obj_id": disbursement.obj_id
        })
        self._transaction.run(stmt)
        return disbursement.obj_id

    def get_organization(self, node: ET.Element) -> Organization:
        name: str = self._get_narrative(node)
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
        stmt = Stat.create_node(org.get_name(), "Organization", {
            "name": org.name, "ref": org.ref, "type": org.type,
            "obj_id": org.obj_id
        })
        self._transaction.run(stmt)
        return org.obj_id

    def get_policy(self, node: ET.Element) -> Policy:
        name: str = self._get_narrative(node)
        vocabulary: int = int(node.get("vocabulary"))
        code: int = int(node.get("code"))
        significance: int = int(node.get("significance"))
        return Policy(name, vocabulary, code, significance)

    def add_policy(self, policy: Policy) -> int:
        stmt = Stat.create_node(policy.get_name(), "Policy", {
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
        stmt = Stat.create_node(location.get_name(), "Location", {
            "code": location.code, "name": location.name,
            "obj_id": location.obj_id
        })
        self._transaction.run(stmt)
        return location.obj_id

    def get_transactions(self, node: ET.Element, orgs: Iterable[Organization] = None) -> List[Transaction]:
        transactions: List[Transaction] = []
        for transaction_node in node.iter("transaction"):
            transaction_node: ET.Element = transaction_node
            ty = int(transaction_node.find("transaction-type").get("code"))
            date = transaction_node.find("transaction-date").get("iso-date")
            value_node: ET.Element = transaction_node.find("value")
            value = int(value_node.text)
            value_date = value_node.get("value-date")
            provider_node: ET.Element = transaction_node.find("provider-org")
            provider_ref = provider_node.get("ref")
            provider_name = self._get_narrative(provider_node)
            receiver_node: ET.Element = transaction_node.find("receiver-org")
            receiver_ref = receiver_node.get("ref")
            receiver_name = self._get_narrative(receiver_node)
            transaction = Transaction(ty, date, value, value_date, provider_ref, provider_name,
                                      receiver_ref, receiver_name, orgs)
            transactions.append(transaction)
        return transactions


class EdgeAttr:
    @staticmethod
    def get_activity_attributes(activity: Activity) -> Dict[str, Any]:
        act_attr_dict: Dict[str, str] = dict()
        for act_date in activity.dates:
            act_attr_dict["date_type_{}".format(act_date.type)] = act_date.date
        return act_attr_dict


def main():
    driver: neo.Driver = GraphDatabase.driver("bolt://" + SERVER_URL, auth=basic_auth(AUTH_USER, AUTH_PASSWORD))
    session: neo.Session = driver.session()

    print("--- Task started ---")
    print(timestr())

    print("Clearing indices...")
    # This operation could fail at bootstrap
    trans: neo.Transaction = session.begin_transaction()
    for class_name in CLASS_LIST:
        trans.run("DROP INDEX ON :{}(obj_id);".format(class_name))
    try:
        trans.commit()
    except neo_ex.DatabaseError as ex:
        print(ex.message)
        trans.rollback()
    finally:
        trans.close()

    print("Clearing nodes and relations...")
    trans = session.begin_transaction()
    trans.run("MATCH (n) DETACH DELETE n;")
    trans.commit()
    trans.close()

    ext = SessionExtension(session)

    print("Creating indices...")
    # https://stackoverflow.com/questions/24875665/how-to-bulk-insert-relationships
    trans = session.begin_transaction()
    for class_name in CLASS_LIST:
        trans.run("CREATE INDEX ON :{}(obj_id);".format(class_name))
    trans.commit()
    trans.close()

    def process_xml(file: str) -> None:
        tree = ET.ElementTree(file=file)

        ext.begin_transaction()

        print("Adding activities for '{}'... ({})".format(file, timestr()))
        for activity_node in tree.iter("iati-activity"):
            activity_node: ET.Element = activity_node

            # Activity
            activity = ext.get_activity(activity_node)
            ext.add_activity(activity)
            # Budget
            budget_node: ET.Element = activity_node.find("budget")
            budget = ext.get_budget(budget_node, activity)
            ext.add_budget(budget)
            # Planned disbursements
            disbs: List[Disbursement] = []
            disbursement_index = 0
            for disbursement_node in activity_node.iter("planned-disbursement"):
                disbursement = ext.get_disbursement(disbursement_node, activity, disbursement_index)
                ext.add_disbursement(disbursement)
                disbursement_index += 1
                disbs.append(disbursement)
            # Organizations
            orgs: List[Organization] = []
            reporting_org_node: ET.Element = activity_node.find("reporting-org")
            organization = ext.get_organization(reporting_org_node)
            orgs.append(organization)
            ext.add_organization(organization)
            for participating_org_node in activity_node.iter("participating-org"):
                organization = ext.get_organization(participating_org_node)
                ext.add_organization(organization)
                orgs.append(organization)
            # Policy markers
            policies: List[Policy] = []
            for policy_marker_node in activity_node.iter("policy-marker"):
                policy = ext.get_policy(policy_marker_node)
                ext.add_policy(policy)
                policies.append(policy)
            # Locations
            recipient_node = activity_node.find("recipient-country")
            if recipient_node is None:
                recipient_node = activity_node.find("recipient-region")
            location = ext.get_location(recipient_node)
            ext.add_location(location)

            def add_relations():
                # Initialize transaction list.
                # transactions = ext.get_transactions(activity_node, orgs)

                # (Activity) -[Commits]-> (Budget)
                stmt = Stat.create_edge_by_ids("act", "Activity", activity.obj_id, "budget", "Budget", budget.obj_id,
                                               "Commits")
                ext.run(stmt)

                # (Activity) -[Is_For_Location]-> (Location)
                stmt = Stat.create_edge_by_ids("act", "Activity", activity.obj_id, "loc", "Location", location.obj_id,
                                               "Is_For", EdgeAttr.get_activity_attributes(activity))
                ext.run(stmt)

                # (Activity) -[Supports]-> (Policy)
                for pol in policies:
                    stmt = Stat.create_edge_by_ids("act", "Activity", activity.obj_id, "pol", "Policy", pol.obj_id,
                                                   "Supports", EdgeAttr.get_activity_attributes(activity))
                    ext.run(stmt)

                # (Organization) -[Participates_In/Manages]-> (Activity)
                for i, org in enumerate(orgs):
                    stmt = Stat.create_edge_by_ids("org", "Organization", org.obj_id, "act", "Activity",
                                                   activity.obj_id,
                                                   "Participates_In" if i > 0 else "Manages",
                                                   # TODO: a better word/phrase?
                                                   EdgeAttr.get_activity_attributes(activity))
                    ext.run(stmt)

                # (Organization) -[?]-> (Disbursement)

                # (Organization) -[Implements]-> (Policy)
                for i, (org, pol) in enumerate(zip(orgs, policies)):
                    if i > 0:
                        # Don't mess up with reporting organization.
                        stmt = Stat.create_edge_by_ids("org", "Organization", org.obj_id, "pol", "Policy", pol.obj_id,
                                                       "Implements")
                        ext.run(stmt)

                # (Organization) -[?]-> (Location)

                # (Budget) -[Disburses]-> (Disbursement)
                for dis in disbs:
                    stmt = Stat.create_edge_by_ids("bud", "Budget", budget.obj_id, "dis", "Disbursement",
                                                   dis.obj_id,
                                                   "Disburses")
                    ext.run(stmt)

                # (Budget) -[?]-> (Location)

                # (Policy) -[Commits] -> (Budget)
                for pol in policies:
                    stmt = Stat.create_edge_by_ids("pol", "Policy", pol.obj_id, "bud", "Budget", budget.obj_id,
                                                   "Commits")
                    ext.run(stmt)

                    # (Policy) -[?]-> (Location)

            add_relations()

        print("Committing...")
        ext.commit()

    for xml_file in XML_FILES:
        process_xml(xml_file)

    session.close()

    print("--- Task completed ---")
    print(timestr())


if __name__ == '__main__':
    main()
