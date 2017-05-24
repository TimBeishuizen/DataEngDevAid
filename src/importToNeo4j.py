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
    from src.EdgeAttr import EdgeAttr
else:
    # So do a trick, use the standard Python 3 import syntax to feed PyCharm's intellisense.
    from .CypherStatementBuilder import CypherStatementBuilder as Stmt
    from .Entities import *
    from .SessionExtension import SessionExtension
    from .EdgeAttr import EdgeAttr

SERVER_URL = "localhost:7687"
AUTH_USER = "neo4j"
AUTH_PASSWORD = "neo"

CLASS_LIST = ["Activity", "Budget", "Organization", "Policy", "Location"]
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

            def add_nodes():
                # Activity
                activity = ext.get_activity(activity_node)
                ext.add_activity(activity)

                # Budget
                budget_node: ET.Element = activity_node.find("budget")
                budget = ext.get_budget(budget_node, activity)
                ext.add_budget(budget)

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

                return activity, budget, organizations, policies, location

            activity, budget, organizations, policies, location = add_nodes()

            def add_relations(activity: Activity, budget: Budget, organizations: List[Organization],
                              policies: List[Policy], location: Location):
                # Initialize transaction list and disbursement list.
                transactions = EdgeAttr.get_transactions(activity_node, organizations)
                disbursements = EdgeAttr.get_disbursements(activity_node, activity)

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
                    # Ignore the policies whose significance level is 0 ("not targeted").
                    if pol.significance > 0:
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

                # (Organization) -[Implements]-> (Policy)
                for i, org in enumerate(organizations):
                    # 1. Ignore the first organization (reporting-org, always Ministry of Foreign Affairs).
                    # 2. Ignore the Ministry's appearance in all participating organizations.
                    if i == 0 or org.ref == "XM-DAC-7":
                        continue
                    for pol in policies:
                        if pol.significance > 0:
                            stmt = Stmt.create_edge_by_ids("org", "Organization", org.obj_id,
                                                           "pol", "Policy", pol.obj_id,
                                                           "Implements")
                            ext.run(stmt)

                # (Organization) -[?]-> (Location)

                # (Budget) -[Disburses_To]-> (Organization)
                for i, org in enumerate(organizations):
                    if i == 0 or org.ref == "XM-DAC-7":
                        continue
                    for disbursement in disbursements:
                        stmt = Stmt.create_edge_by_ids("bud", "Budget", budget.obj_id,
                                                       "org", "Organization", org.obj_id,
                                                       "Disburses_To",
                                                       EdgeAttr.get_disbursement_attributes(disbursement))
                        ext.run(stmt)

                # (Budget) -[Transfers_To]-> (Organization)
                for i, org in enumerate(organizations):
                    if i == 0 or org.ref == "XM-DAC-7":
                        continue
                    for transaction in transactions:
                        # Here we use the "receiver-org" of transaction node instead of "participating-org" of
                        # activity node.
                        if transaction.receiver_org is None:
                            if TRANSACTION_DEBUG:
                                print("[WARN] Cannot create relation 'transfers to' between budget and organization, "
                                      "having a transaction as attribute. Receiver name={}"
                                      .format(transaction.receiver_name))
                            continue
                        if transaction.receiver_org.obj_id == org.obj_id:
                            stmt = Stmt.create_edge_by_ids("bud", "Budget", budget.obj_id,
                                                           "org", "Organization", org.obj_id,
                                                           "Transfers_To",
                                                           EdgeAttr.get_transaction_attributes(transaction))
                            ext.run(stmt)

                # (Budget) -[?]-> (Location)

                # (Policy) -[?]-> (Location)

                # (Policy) -[Commits] -> (Budget)
                for pol in policies:
                    if pol.significance > 0:
                        stmt = Stmt.create_edge_by_ids("pol", "Policy", pol.obj_id,
                                                       "bud", "Budget", budget.obj_id,
                                                       "Commits")
                        ext.run(stmt)

            add_relations(activity, budget, organizations, policies, location)

        print("Committing...")
        ext.commit()

    for xml_file in XML_FILES:
        process_xml(xml_file)

    session.close()

    print("--- Task completed ---")
    print(timestr())


if __name__ == '__main__':
    main()
