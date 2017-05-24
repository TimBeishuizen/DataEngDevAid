from typing import Dict, Any
from xml.etree import ElementTree as ET
from time import localtime, strftime
import neo4j.v1 as neo
import neo4j.exceptions as neo_ex
from neo4j.v1 import GraphDatabase, basic_auth

if __name__ == "__main__":
    # The main module must import files from the same directory in this way, but PyCharm just can't recognize it.
    # http://stackoverflow.com/questions/41816973/modulenotfounderror-what-does-it-mean-main-is-not-a-package
    from src.CypherStatementBuilder import CypherStatementBuilder as Stmt
    from src.Entities import *
    from src.SessionExtension import SessionExtension
else:
    # So do a trick, use the standard Python 3 import syntax to feed PyCharm's intellisense.
    from .CypherStatementBuilder import CypherStatementBuilder as Stmt
    from .Entities import *
    from .SessionExtension import SessionExtension

SERVER_URL = "localhost:7687"
AUTH_USER = "neo4j"
AUTH_PASSWORD = "neo"

CLASS_LIST = ["Activity", "Budget", "Disbursement", "Organization", "Policy", "Location"]
XML_FILES = [
    "../data/IATIACTIVITIES19972007.xml"  # ,
    # "../data/IATIACTIVITIES20082009.xml",
    # "../data/IATIACTIVITIES20102011.xml",
    # "../data/IATIACTIVITIES20122013.xml",
    # "../data/IATIACTIVITIES20142015.xml",
    # "../data/IATIACTIVITIES20162017.xml"
]


def timestr() -> str:
    return strftime("%Y-%m-%d %H:%M:%S", localtime())


class EdgeAttr:
    @staticmethod
    def get_activity_attributes(activity: Activity) -> Dict[str, Any]:
        act_attr_dict: Dict[str, str] = dict()
        for act_date in activity.dates:
            act_attr_dict["date_type_{}".format(act_date.type)] = act_date.date
        return act_attr_dict

    @staticmethod
    def get_transactions(node: ET.Element, organizations: Iterable[Organization] = None) -> List[Transaction]:
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
            provider_name = SessionExtension.narrative(provider_node)
            receiver_node: ET.Element = transaction_node.find("receiver-org")
            receiver_ref = receiver_node.get("ref")
            receiver_name = SessionExtension.narrative(receiver_node)
            transaction = Transaction(ty, date, value, value_date, provider_ref, provider_name,
                                      receiver_ref, receiver_name, organizations)
            transactions.append(transaction)
        return transactions


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

            activity: Activity = None
            budget: Budget = None
            disbursements: List[Disbursement] = None
            organizations: List[Organization] = None
            policies: List[Policy] = None
            location: Location = None

            def add_nodes():
                # Activity
                activity = ext.get_activity(activity_node)
                ext.add_activity(activity)

                # Budget
                budget_node: ET.Element = activity_node.find("budget")
                budget = ext.get_budget(budget_node, activity)
                ext.add_budget(budget)

                # Planned disbursements
                disbursements = []
                disbursement_index = 0
                for disbursement_node in activity_node.iter("planned-disbursement"):
                    disbursement = ext.get_disbursement(disbursement_node, activity, disbursement_index)
                    ext.add_disbursement(disbursement)
                    disbursement_index += 1
                    disbursements.append(disbursement)

                # Organizations
                organizations = []
                # First the reporting organization (always Ministry of Foreign Affairs)
                reporting_org_node: ET.Element = activity_node.find("reporting-org")
                organization = ext.get_organization(reporting_org_node)
                organizations.append(organization)
                ext.add_organization(organization)
                # Then the participating organizations
                for participating_org_node in activity_node.iter("participating-org"):
                    organization = ext.get_organization(participating_org_node)
                    ext.add_organization(organization)
                    organizations.append(organization)

                # Policy markers
                policies = []
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

                return activity, budget, disbursements, organizations, policies, location

            activity, budget, disbursements, organizations, policies, location = add_nodes()

            def add_relations(activity: Activity, budget: Budget, disbursements: List[Disbursement],
                              organizations: List[Organization], policies: List[Policy], location: Location):
                # Initialize transaction list.
                # transactions = EdgeAttr.get_transactions(activity_node, orgs)

                # (Activity) -[Commits]-> (Budget)
                stmt = Stmt.create_edge_by_ids("act", "Activity", activity.obj_id,
                                               "budget", "Budget", budget.obj_id,
                                               "Commits")
                ext.run(stmt)

                # (Activity) -[Is_For_Location]-> (Location)
                stmt = Stmt.create_edge_by_ids("act", "Activity", activity.obj_id,
                                               "loc", "Location", location.obj_id,
                                               "Is_For", EdgeAttr.get_activity_attributes(activity))
                ext.run(stmt)

                # (Activity) -[Supports]-> (Policy)
                for pol in policies:
                    stmt = Stmt.create_edge_by_ids("act", "Activity", activity.obj_id,
                                                   "pol", "Policy", pol.obj_id,
                                                   "Supports", EdgeAttr.get_activity_attributes(activity))
                    ext.run(stmt)

                # (Organization) -[Participates_In]-> (Activity)
                for i, org in enumerate(organizations):
                    # 1. Ignore the first organization (reporting-org, always Ministry of Foreign Affairs).
                    # 2. Ignore the Ministry's appearance in all participating organizations.
                    if i > 0 and org.ref != "XM-DAC-7":
                        stmt = Stmt.create_edge_by_ids("org", "Organization", org.obj_id,
                                                       "act", "Activity", activity.obj_id,
                                                       "Participates_In", EdgeAttr.get_activity_attributes(activity))
                        ext.run(stmt)

                # (Organization) -[?]-> (Disbursement)

                # (Organization) -[Implements]-> (Policy)
                for i, (org, pol) in enumerate(zip(organizations, policies)):
                    # 1. Ignore the first organization (reporting-org, always Ministry of Foreign Affairs).
                    # 2. Ignore the Ministry's appearance in all participating organizations.
                    if i > 0 and org.ref != "XM-DAC-7":
                        stmt = Stmt.create_edge_by_ids("org", "Organization", org.obj_id,
                                                       "pol", "Policy", pol.obj_id,
                                                       "Implements")
                        ext.run(stmt)

                # (Organization) -[?]-> (Location)

                # (Budget) -[Disburses]-> (Disbursement)
                for dis in disbursements:
                    stmt = Stmt.create_edge_by_ids("bud", "Budget", budget.obj_id,
                                                   "dis", "Disbursement", dis.obj_id,
                                                   "Disburses")
                    ext.run(stmt)

                # (Budget) -[?]-> (Location)

                # (Policy) -[Commits] -> (Budget)
                for pol in policies:
                    stmt = Stmt.create_edge_by_ids("pol", "Policy", pol.obj_id,
                                                   "bud", "Budget", budget.obj_id,
                                                   "Commits")
                    ext.run(stmt)

                    # (Policy) -[?]-> (Location)

            add_relations(activity, budget, disbursements, organizations, policies, location)

        print("Committing...")
        ext.commit()

    for xml_file in XML_FILES:
        process_xml(xml_file)

    session.close()

    print("--- Task completed ---")
    print(timestr())


if __name__ == '__main__':
    main()
