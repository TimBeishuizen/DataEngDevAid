from typing import List, Dict, Tuple

import neo4j.v1 as neo
from neo4j.v1 import GraphDatabase, basic_auth

try:
    from CypherStatementBuilder import *
except ImportError:
    from .CypherStatementBuilder import *

"""
╒════════════════════════════════════════════╤════════╕
│"n.name"                                    │"n.code"│
╞════════════════════════════════════════════╪════════╡
│"NORTH OF SAHARA, REGIONAL"                 │"189"   │
│"SOUTH OF SAHARA, REGIONAL"                 │"289"   │
│"AFRICA, REGIONAL"                          │"298"   │
│"North & Central America, regional"         │"389"   │
│"SOUTH AMERICA, REGIONAL"                   │"489"   │
│"AMERICA, REGIONAL"                         │"498"   │
│"MIDDLE EAST, REGIONAL"                     │"589"   │
│"CENTRAL ASIA, REGIONAL"                    │"619"   │
│"SOUTH ASIA, REGIONAL"                      │"679"   │
│"FAR EAST ASIA, REGIONAL"                   │"789"   │
│"ASIA, REGIONAL"                            │"798"   │
│"EUROPE, REGIONAL"                          │"89"    │
│"WORLD WIDE"                                │"998"   │
├────────────────────────────────────────────┼────────┤
│"AFGHANISTAN"                               │"AF"    │
│"ALBANIA"                                   │"AL"    │
│"ARMENIA"                                   │"AM"    │
│"ANGOLA"                                    │"AO"    │
│"ARGENTINA"                                 │"AR"    │
│"AZERBAIJAN"                                │"AZ"    │
│"BOSNIA AND HERZEGOVINA"                    │"BA"    │
│"BANGLADESH"                                │"BD"    │
│"BURKINA FASO"                              │"BF"    │
│"BURUNDI"                                   │"BI"    │
│"BENIN"                                     │"BJ"    │
│"BOLIVIA, PLURINATIONAL STATE OF"           │"BO"    │
│"BRAZIL"                                    │"BR"    │
│"BHUTAN"                                    │"BT"    │
│"BOTSWANA"                                  │"BW"    │
│"BELARUS"                                   │"BY"    │
│"CONGO, THE DEMOCRATIC REPUBLIC OF THE"     │"CD"    │
│"CENTRAL AFRICAN REPUBLIC"                  │"CF"    │
│"CONGO"                                     │"CG"    │
│"COTE D'IVOIRE"                             │"CI"    │
│"CHILE"                                     │"CL"    │
│"CAMEROON"                                  │"CM"    │
│"CHINA"                                     │"CN"    │
│"COLOMBIA"                                  │"CO"    │
│"COSTA RICA"                                │"CR"    │
│"CUBA"                                      │"CU"    │
│"CAPE VERDE"                                │"CV"    │
│"DOMINICAN REPUBLIC"                        │"DO"    │
│"ALGERIA"                                   │"DZ"    │
│"ECUADOR"                                   │"EC"    │
│"EGYPT"                                     │"EG"    │
│"ERITREA"                                   │"ER"    │
│"ETHIOPIA"                                  │"ET"    │
│"GEORGIA"                                   │"GE"    │
│"GHANA"                                     │"GH"    │
│"GUINEA"                                    │"GN"    │
│"GUATEMALA"                                 │"GT"    │
│"HONDURAS"                                  │"HN"    │
│"CROATIA"                                   │"HR"    │
│"HAITI"                                     │"HT"    │
│"INDONESIA"                                 │"ID"    │
│"INDIA"                                     │"IN"    │
│"IRAQ"                                      │"IQ"    │
│"IRAN, ISLAMIC REPUBLIC OF"                 │"IR"    │
│"JORDAN"                                    │"JO"    │
│"KENYA"                                     │"KE"    │
│"KYRGYZSTAN"                                │"KG"    │
│"CAMBODIA"                                  │"KH"    │
│"KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF"    │"KP"    │
│"KAZAKHSTAN"                                │"KZ"    │
│"LAO PEOPLE'S DEMOCRATIC REPUBLIC"          │"LA"    │
│"LEBANON"                                   │"LB"    │
│"SRI LANKA"                                 │"LK"    │
│"LIBERIA"                                   │"LR"    │
│"LIBYAN ARAB JAMAHIRIYA"                    │"LY"    │
│"MOROCCO"                                   │"MA"    │
│"MOLDOVA, REPUBLIC OF"                      │"MD"    │
│"MONTENEGRO"                                │"ME"    │
│"MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF"│"MK"    │
│"MALI"                                      │"ML"    │
│"MYANMAR"                                   │"MM"    │
│"MONGOLIA"                                  │"MN"    │
│"MAURITANIA"                                │"MR"    │
│"MALAWI"                                    │"MW"    │
│"MEXICO"                                    │"MX"    │
│"MALAYSIA"                                  │"MY"    │
│"MOZAMBIQUE"                                │"MZ"    │
│"NAMIBIA"                                   │"NA"    │
│"NIGER"                                     │"NE"    │
│"NIGERIA"                                   │"NG"    │
│"NICARAGUA"                                 │"NI"    │
│"NEPAL"                                     │"NP"    │
│"PANAMA"                                    │"PA"    │
│"PERU"                                      │"PE"    │
│"PAPUA NEW GUINEA"                          │"PG"    │
│"PHILIPPINES"                               │"PH"    │
│"PAKISTAN"                                  │"PK"    │
│"PALESTINIAN TERRITORY, OCCUPIED"           │"PS"    │
│"PARAGUAY"                                  │"PY"    │
│"SERBIA"                                    │"RS"    │
│"RWANDA"                                    │"RW"    │
│"SUDAN"                                     │"SD"    │
│"SIERRA LEONE"                              │"SL"    │
│"SENEGAL"                                   │"SN"    │
│"SOMALIA"                                   │"SO"    │
│"SURINAME"                                  │"SR"    │
│"SOUTH SUDAN"                               │"SS"    │
│"EL SALVADOR"                               │"SV"    │
│"SYRIAN ARAB REPUBLIC"                      │"SY"    │
│"CHAD"                                      │"TD"    │
│"THAILAND"                                  │"TH"    │
│"TAJIKISTAN"                                │"TJ"    │
│"TUNISIA"                                   │"TN"    │
│"TURKEY"                                    │"TR"    │
│"TANZANIA, UNITED REPUBLIC OF"              │"TZ"    │
│"UKRAINE"                                   │"UA"    │
│"UGANDA"                                    │"UG"    │
│"URUGUAY"                                   │"UY"    │
│"VENEZUELA, BOLIVARIAN REPUBLIC OF"         │"VE"    │
│"VIET NAM"                                  │"VN"    │
│"VANUATU"                                   │"VU"    │
│"KOSOVO"                                    │"XK"    │
│"YEMEN"                                     │"YE"    │
│"SOUTH AFRICA"                              │"ZA"    │
│"ZAMBIA"                                    │"ZM"    │
│"ZIMBABWE"                                  │"ZW"    │
└────────────────────────────────────────────┴────────┘
"""

SERVER_HOST = "localhost"
SERVER_PORT = 7687
AUTH_USER = "neo4j"
AUTH_PASSWORD = "neo"

country_region_map: Dict[int, List[str]] = {
    189: [  # North Sahara
        "DZ", "EG", "ER", "ET", "LY", "MA", "MR", "TN"
    ], 289: [  # South Sahara
        "AO", "BF", "BI", "BJ", "BW", "CD", "CF", "CG", "CI", "CM", "CV", "GH", "GN", "LR", "ML", "MW", "MZ",
        "NA", "NE", "NG", "RW", "SD", "SL", "SN", "SO", "SS", "TD", "TZ", "UG", "ZA", "ZM", "ZW"
    ], 298: [  # Africa
    ], 389: [  # North/Central America
        "CR", "CU", "DO", "GT", "HN", "HT", "KE", "MX", "NI", "PA", "SV", "VE"
    ], 489: [  # South America
        "AR", "BO", "BR", "CL", "CO", "EC", "PE", "PY", "SR", "UY"
    ], 498: [  # America
    ], 589: [  # Middle East
        "IQ", "IR", "JO", "LB", "PS", "SY", "TR"
    ], 619: [  # Central Asia
        "KG", "KZ", "MN", "TJ", "YE"
    ], 679: [  # South  Asia
        "AF", "BD", "BT", "ID", "IN", "KH", "LA", "LK", "MM", "MY", "NP", "PH", "PK", "TH", "VN"
    ], 789: [  # Far East Asia
        "CN", "KP"
    ], 798: [  # Asia
    ], 89: [  # Europe
        "AL", "AM", "AZ", "BA", "BY", "GE", "HR", "MD", "ME", "MK", "RS", "UA", "XK",
    ], 998: [  # World Wide
        "PG", "VU"  # Oceania
    ]
}

if __name__ == '__main__':
    server_url = "bolt://{}:{}".format(SERVER_HOST, SERVER_PORT)
    driver: neo.Driver = GraphDatabase.driver(server_url, auth=basic_auth(AUTH_USER, AUTH_PASSWORD))
    session: neo.Session = driver.session()

    trans: neo.Transaction = session.begin_transaction()
    query = "MATCH ()-[b:BELONGS_TO]-() DETACH DELETE b;"
    trans.run(query)
    trans.commit()

    belongs_to_tpl = "MATCH (c:Location), (r:Location) WHERE c.code={} AND r.code={} " \
                     "CREATE (c)-[:BELONGS_TO]->(r);"

    trans = session.begin_transaction()
    for kv1 in country_region_map.items():
        region_code: str = kv1[0]
        country_list: List[str] = kv1[1]

        for country_code in country_list:
            query = belongs_to_tpl.format(get_escaped_str(str(country_code)), get_escaped_str(str(region_code)))
            trans.run(query)
    trans.commit()

    trans = session.begin_transaction()
    other_belongings: List[Tuple[int, int]] = [
        (189, 298), (289, 298), (389, 498), (489, 498), (589, 798), (619, 798), (679, 798), (789, 798),
        (298, 998), (498, 998), (798, 998), (89, 998)
    ]
    for country_code, master_region_code in other_belongings:
        query = belongs_to_tpl.format(get_escaped_str(str(country_code)), get_escaped_str(str(master_region_code)))
        trans.run(query)
    trans.commit()

    session.close()

"""
# Query for a sub graph:  (:Budget)<-[:COMMITS]-(:Activity)-[:EXECUTED_IN]->(:Locations)

with 20060101 as _1
match (act:Activity)-[:EXECUTED_IN]->(loc:Location)
match (act:Activity)-[com:COMMITS]->(bud:Budget)
match (loc:Location)-[:BELONGS_TO]->(loc2:Location)
with distinct _1, act, loc, loc2, bud, com
where _1 <= com.period_start < _1 + 10000
return act, bud, loc, loc2, com
"""

"""
# Summing the amount of budgets of a country (Mali in this example) would receive in a year's period.

with 20140101 as _1
match (act:Activity)-[:EXECUTED_IN]->(loc:Location {code:'ML'})
match (act:Activity)-[com:COMMITS]->(bud:Budget)
with distinct _1, act, loc, bud, com
where _1 <= com.period_start < _1 + 10000
return act, bud, loc, com, sum(bud.value) as amount
order by amount
"""

"""
Basically, there is no correlation between budgets planned to a location in one year, and the next year. Aids targeting
Africa, esp. South Sahara have the largest amount and densest edges, partly because this region has the biggest number
of target countries. According to the data we have, the shape of activity-budget-location graph is a tree, and no
interactions among locations is recorded. There are some interesting results (e.g. single-way sawteeth of amount budget
targeting one country) but all of them can be done statistically, without utilizing the graph structure. Although the
locations are connected, a long path is still hard to find.
Conclusion: activity-budget-location graph is not useful as expected.
"""
