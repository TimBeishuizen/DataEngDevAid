from typing import List
from xml.etree import ElementTree as ET
from time import localtime, strftime
import neo4j.v1 as neo
import neo4j.exceptions as neo_ex
from neo4j.v1 import GraphDatabase, basic_auth
import csv

try:
    # The main module must import files from the same directory in this way, but PyCharm just can't recognize it.
    # http://stackoverflow.com/questions/41816973/modulenotfounderror-what-does-it-mean-main-is-not-a-package
    from CypherStatementBuilder import CypherStatementBuilder as Stmt
    from Entities import *
    from SessionExtension import SessionExtension
    from EdgeAttr import EdgeAttr
except ImportError:
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

    ext = SessionExtension(session)

    print("--- Task started ---")
    print(timestr())

    print("Clearing indices...")
    # This operation could fail at bootstrap
    for class_name in CLASS_LIST:
        try:
            ext.run_session("DROP INDEX ON :{}(obj_id);".format(class_name))
        except neo_ex.DatabaseError as ex:
            print(ex.message)

    print("Clearing nodes and relations...")
    ext.run_session("MATCH (n) DETACH DELETE n;")

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

                # policy code -> significance
                policy_significance_map: Dict[int, int] = dict()
                # Policy markers
                policies = []
                for policy_marker_node in activity_node.iter("policy-marker"):
                    policy = ext.get_policy(policy_marker_node)
                    policy_significance_map[policy.code] = int(policy_marker_node.get("significance"))
                    ext.add_policy(policy)
                    policies.append(policy)

                # Locations
                recipient_node = activity_node.find("recipient-country")
                if recipient_node is None:
                    recipient_node = activity_node.find("recipient-region")
                location = ext.get_location(recipient_node)
                ext.add_location(location)

                return activity, budget, organizations, policies, location, policy_significance_map

            t_activity, t_budget, t_organizations, t_policies, t_location, t_psm = add_nodes()

            def add_relations(activity: Activity, budget: Budget, organizations: List[Organization],
                              policies: List[Policy], location: Location, policy_significance_map: Dict[int, int]):
                # Initialize transaction list and disbursement list.
                transactions = EdgeAttr.get_transactions(activity_node, organizations)
                disbursements = EdgeAttr.get_disbursements(activity_node, activity)

                def get_pol_sig(code: int) -> int:
                    return policy_significance_map.get(code, 0)

                # (Activity) -[Commits]-> (Budget)
                stmt = Stmt.create_edge_by_ids("act", "Activity", activity.obj_id,
                                               "budget", "Budget", budget.obj_id,
                                               "Commits", EdgeAttr.commits(budget))
                ext.run(stmt)

                # (Activity) -[Executed_In]-> (Location)
                stmt = Stmt.create_edge_by_ids("act", "Activity", activity.obj_id,
                                               "loc", "Location", location.obj_id,
                                               "Executed_In", EdgeAttr.executed_in(activity))
                ext.run(stmt)

                # (Organization) -[Implements]-> (Policy)
                for i, org in enumerate(organizations):
                    # 1. Ignore the first organization (reporting-org, always Ministry of Foreign Affairs).
                    # 2. Ignore the Ministry's appearance in all participating organizations.
                    if i == 0 or org.ref == "XM-DAC-7":
                        continue
                    for pol in policies:
                        if get_pol_sig(pol.code) > 0:
                            # Note that the relation between a specific pair of organization and policy is unique.
                            stmt = Stmt.create_edge_by_ids("org", "Organization", org.obj_id,
                                                           "pol", "Policy", pol.obj_id,
                                                           "Implements", EdgeAttr.implements(),
                                                           is_unique=True)
                            ext.run(stmt)

                # (Budget) -[Transacts]-> (Organization)
                for i, org in enumerate(organizations):
                    if i == 0 or org.ref == "XM-DAC-7":
                        continue
                    for transaction in transactions:
                        # Type 2 = commitment, ignore it. Just keep the real transactions (type = 3).
                        if transaction.type == 2:
                            continue
                        # Here we use the "receiver-org" of transaction node instead of "participating-org" of
                        # activity node.
                        if transaction.receiver_org is None:
                            if TRANSACTION_DEBUG:
                                print(
                                    "[WARN] Cannot create relation 'transfers to' between budget and organization, "
                                    "having a transaction as attribute. Receiver name={}".format(
                                        transaction.receiver_name))
                            continue
                        if transaction.receiver_org.obj_id == org.obj_id:
                            stmt = Stmt.create_edge_by_ids("bud", "Budget", budget.obj_id,
                                                           "org", "Organization", org.obj_id,
                                                           "Transacts",
                                                           EdgeAttr.transacts(transaction))
                            ext.run(stmt)

                # (Budget) -[Plans_Disbursement]-> (Organization)
                for i, org in enumerate(organizations):
                    if i == 0 or org.ref == "XM-DAC-7":
                        continue
                    for disbursement in disbursements:
                        stmt = Stmt.create_edge_by_ids("bud", "Budget", budget.obj_id,
                                                       "org", "Organization", org.obj_id,
                                                       "Plans_Disbursement",
                                                       EdgeAttr.plans_disbursement(disbursement))
                        ext.run(stmt)

                # (Activity) -[Supports]-> (Policy)
                for pol in policies:
                    # Ignore the policies whose significance level is 0 ("not targeted").
                    if get_pol_sig(pol.code) > 0:
                        stmt = Stmt.create_edge_by_ids("act", "Activity", activity.obj_id,
                                                       "pol", "Policy", pol.obj_id,
                                                       "Supports",
                                                       EdgeAttr.supports(activity, pol, policy_significance_map))
                        ext.run(stmt)

                # (Organization) -[Participates_In]-> (Activity)
                for i, org in enumerate(organizations):
                    # 1. Ignore the first organization (reporting-org, always Ministry of Foreign Affairs).
                    # 2. Ignore the Ministry's appearance in all participating organizations.
                    if i > 0 and org.ref != "XM-DAC-7":
                        stmt = Stmt.create_edge_by_ids("org", "Organization", org.obj_id,
                                                       "act", "Activity", activity.obj_id,
                                                       "Participates_In", EdgeAttr.participates_in(activity))
                        ext.run(stmt)

                # (Policy) -[Funds] -> (Budget)
                for pol in policies:
                    if get_pol_sig(pol.code) > 0:
                        stmt = Stmt.create_edge_by_ids("pol", "Policy", pol.obj_id,
                                                       "bud", "Budget", budget.obj_id,
                                                       "Funds", EdgeAttr.funds(budget))
                        ext.run(stmt)

            add_relations(t_activity, t_budget, t_organizations, t_policies, t_location, t_psm)

        print("Committing...")
        ext.commit()

    for xml_file in XML_FILES:
        process_xml(xml_file)

    def generate_csv(sess: neo.Session):
        print("Find all nodes ({})".format(timestr()))
        nodes = []
        result = sess.run("MATCH (n) RETURN EXTRACT(key IN keys(n) | {value: n[key], key:key})")
        for record in result:
            node = []
            pre_node = record[0]
            for item in pre_node:
                if item['key'] == 'obj_id':
                    node.insert(0, item['value'])
                else:
                    node.append(item['key'])
                    if isinstance(item['value'], str):
                        node.append(item['value'].encode('ascii', 'ignore'))
                    else:
                        node.append(item['value'])
            nodes.append(node)

        print("Save nodes in csv file ({})".format(timestr()))
        with open('nodes.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            for node in nodes:
                csvwriter.writerow(node)

        # Save edges
        print("Find all edges ({})".format(timestr()))
        edges = []
        edge_count = 0
        result = sess.run("MATCH (n1) -[t]- (n2) RETURN EXTRACT(key IN keys(t) | {value: t[key], key:key}), n1, n2")
        for record in result:
            edge = [edge_count]
            edge_count += 1
            edge.append(record[1]["obj_id"])
            edge.append(record[2]["obj_id"])
            pre_edge = record[0]
            for item in pre_edge:
                if item['key'] == 'obj_id':
                    edge.insert(0, item['value'])
                else:
                    edge.append(item['key'])
                    if isinstance(item['value'], str):
                        edge.append(item['value'].encode('ascii', 'ignore'))
                    else:
                        edge.append(item['value'])
            edges.append(edge)

        print("Save edges in csv file ({})".format(timestr()))
        with open('edges.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            for edge in edges:
                csvwriter.writerow(edge)

    generate_csv(session)

    session.close()

    print("--- Task completed ---")
    print(timestr())


if __name__ == '__main__':
    main()
