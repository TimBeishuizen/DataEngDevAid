import neo4j.v1 as neo
from neo4j.v1 import GraphDatabase, basic_auth

if __name__ == "__main__":
    # The main module must import files from the same directory in this way, but PyCharm just can't recognize it.
    # http://stackoverflow.com/questions/41816973/modulenotfounderror-what-does-it-mean-main-is-not-a-package
    from CypherStatementBuilder import CypherStatementBuilder as CSB
else:
    # So do a trick, use the standard Python 3 import syntax to feed PyCharm's intellisense.
    from .CypherStatementBuilder import CypherStatementBuilder as CSB

SERVER_URL = "localhost:7687"
AUTH_USER = "neo4j"
AUTH_PASSWORD = "neo"

statements = [
    CSB.create_node("TheMatrix", "Movie",
                    {"title": "The Matrix", "released": 1999, "tagline": "Welcome to the Real World"}),

    CSB.create_node("Keanu", "Person", {"name": "Keanu Reeves", "born": 1964}),
    CSB.create_node("Carrie", "Person", {"name": "Carrie-Anne Moss", "born": 1967}),
    CSB.create_node("Laurence", "Person", {"name": "Laurence Fishburne", "born": 1961}),
    CSB.create_node("Hugo", "Person", {"name": "Hugo Weaving", "born": 1960}),
    CSB.create_node("AndyW", "Person", {"name": "Andy Wachowski", "born": 1967}),
    CSB.create_node("LanaW", "Person", {"name": "Lana Wachowski", "born": 1965}),
    CSB.create_node("JoelS", "Person", {"name": "Joel Silver", "born": 1952}),

    CSB.create_edge("Keanu", "TheMatrix", "acted_in", {"roles": ["Neo"]}),
    CSB.create_edge("Carrie", "TheMatrix", "acted_in", {"roles": ["Trinity"]}),
    CSB.create_edge("Laurence", "TheMatrix", "acted_in", {"roles": ["Morpheus"]}),
    CSB.create_edge("Hugo", "TheMatrix", "acted_in", {"roles": ["Agent Smith"]}),
    CSB.create_edge("AndyW", "TheMatrix", "directed"),
    CSB.create_edge("LanaW", "TheMatrix", "directed"),
    CSB.create_edge("JoelS", "TheMatrix", "produced"),

    CSB.create_node("Emil", "Person", {"name": "Emil Eifrem", "born": 1978}),
    CSB.create_edge("Emil", "TheMatrix", "acted_in", {"roles": ["Emil"]})
]

driver: neo.Driver = GraphDatabase.driver("bolt://" + SERVER_URL, auth=basic_auth(AUTH_USER, AUTH_PASSWORD))
session: neo.Session = driver.session()

# The key to success: join statements into ONE query before running them,
# or there will be invalid relations everywhere.
big_stmt = " ".join(statements)
session.run(big_stmt)

session.close()

# Method 2: using the built-in parameterization of neo4j.
# The result is the same as executing statements separately: invalid relations.

# people_data = [
#     {"node": "Keanu", "name": "Keanu Reeves", "born": 1964},
#     {"node": "Carrie", "name": "Carrie-Anne Moss", "born": 1967},
#     {"node": "Laurence", "name": "Laurence Fishburne", "born": 1961},
#     {"node": "Hugo", "name": "Hugo Weaving", "born": 1960},
#     {"node": "AndyW", "name": "Andy Wachowski", "born": 1967},
#     {"node": "LanaW", "name": "Lana Wachowski", "born": 1965},
#     {"node": "JoelS", "name": "Joel Silver", "born": 1952},
#     {"node": "Emil", "name": "Emil Eifrem", "born": 1978}
# ]
#
# relation_data = [
#     {"n1": "Keanu", "edge": "ACTED_IN", "n2": "TheMatrix", "roles": ["Neo"]},
#     {"n1": "Carrie", "edge": "ACTED_IN", "n2": "TheMatrix", "roles": ["Trinity"]},
#     {"n1": "Laurence", "edge": "ACTED_IN", "n2": "TheMatrix", "roles": ["Morpheus"]},
#     {"n1": "Hugo", "edge": "ACTED_IN", "n2": "TheMatrix", "roles": ["Agent Smith"]},
#     {"n1": "AndyW", "edge": "DIRECTED", "n2": "TheMatrix"},
#     {"n1": "LanaW", "edge": "DIRECTED", "n2": "TheMatrix"},
#     {"n1": "JoelS", "edge": "PRODUCED", "n2": "TheMatrix"},
#     {"n1": "Emil", "edge": "ACTED_IN", "n2": "TheMatrix"}
# ]

# trans: neo.Transaction = session.begin_transaction()
# stmt = "CREATE (TheMatrix:Movie {release:1999})"
# trans.run(stmt)
# for person in people_data:
#     trans.run("CREATE (%s:Person {name:{name}, born:{born}})" % person["node"], person)
# for relation in relation_data:
#     if "roles" in relation:
#         trans.run("CREATE (%s)-[:%s {roles:{roles}}]->(%s)" % (relation["n1"], relation["edge"], relation["n2"]),
#                   relation)
#     else:
#         trans.run("CREATE (%s)-[:%s]->(%s)" % (relation["n1"], relation["edge"], relation["n2"]), relation)
# session.commit_transaction()
