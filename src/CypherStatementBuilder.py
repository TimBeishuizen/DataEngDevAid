from typing import Union, Any


def get_escaped_str(v: Union[bool, int, float, str, list, Any]) -> str:
    if v is None:
        # Treat as an empty string.
        return "''"
    elif type(v) is str:
        return "'{}'".format(v.replace("\\", "\\\\").replace("'", "\\'"))
    elif type(v) is list:
        counter = 0
        s = ""
        for item in v:
            if counter == 0:
                s = get_escaped_str(item)
            else:
                s += ", " + get_escaped_str(item)
            counter += 1
        s = "[{}]".format(s)
        return s
    else:
        return str(v)


def get_props_dict_str(props: dict) -> str:
    counter = 0
    props_str = ""
    if props is not None:
        for k, v in props.items():
            if counter == 0:
                props_str = "{}:{}".format(str(k), get_escaped_str(v))
            else:
                props_str += ", {}:{}".format(str(k), get_escaped_str(v))
            counter += 1
    if len(props_str) > 0:
        props_str = " {%s}" % props_str

    return props_str


class CypherStatementBuilder:
    @staticmethod
    def create_node(node_name: str, class_name: str, props: Union[dict, None] = None) -> str:
        """
        Usage:
        create_node("Keanu", "Person", {"name": "Keanu Reeves", "born": 1964})
        :param node_name: Name of the new node.
        :type node_name: str
        :param class_name: The class name of the new node.
        :type class_name: str
        :param props: Node properties.
        :type props: dict
        :return: Generated 'create' statement.
        :rtype: str
        """
        props_str = get_props_dict_str(props)
        create_str = "CREATE ({}:{}{})".format(node_name, class_name, props_str)
        return create_str

    @staticmethod
    def create_edge_by_names(n1_name: str, n2_name: str, edge_class_name: str, edge_props: Union[dict, None] = None,
                             edge_name: str = None, is_unique: bool = False) -> str:
        """
        Usage:
        create_edge("Keanu", "TheMatrix", "acts_in", {"roles": ["Neo"]})
        :param n1_name: Name of node 1 (relation start).
        :type n1_name: str
        :param n2_name: Name of node 2 (relation end).
        :type n2_name: str
        :param edge_class_name: Name of relation class.
        :type edge_class_name: str
        :param edge_name: name of the edge.
        :type edge_name: str
        :param edge_props: Edge properties.
        :type edge_props: dict
        :return: Generated 'create' statement.
        :rtype: str
        """
        props_str = get_props_dict_str(edge_props)
        edge_class_name = edge_class_name.upper()
        create_str = "CREATE {}({})-[{}:{}{}]->({})".format(n1_name, edge_name if edge_name is not None else "",
                                                            "UNIQUE " if is_unique else "",
                                                            edge_class_name, props_str, n2_name)
        return create_str

    @staticmethod
    def create_edge_by_ids(n1_name: str, n1_class: str, n1_id: int, n2_name: str, n2_class: str, n2_id: int,
                           edge_class: str, edge_props: Union[dict, None] = None, edge_name: str = None,
                           is_unique: bool = False) -> str:
        props_str = get_props_dict_str(edge_props)
        edge_class = edge_class.upper()
        create_str = "MATCH ({}:{}), ({}:{}) " \
                     "WHERE {}.obj_id={} AND {}.obj_id={} " \
                     "CREATE {}({})-[{}:{}{}]->({})" \
            .format(n1_name, n1_class, n2_name, n2_class,
                    n1_name, n1_id, n2_name, n2_id,
                    "UNIQUE " if is_unique else "",
                    n1_name, edge_name if edge_name is not None else "", edge_class, props_str, n2_name)
        return create_str


if __name__ == '__main__':
    # Test script.
    q = CypherStatementBuilder
    stmt = q.create_node("Keanu", "Person", {"name": "Keanu Reeves", "born": 1964})
    assert stmt == "CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})"
    stmt = q.create_node("Keanu", "Person", {})
    assert stmt == "CREATE (Keanu:Person)"
    stmt = q.create_node("Keanu", "Person")
    assert stmt == "CREATE (Keanu:Person)"

    # We might also need to check for illegal names and classes...
    # But... well, not implemented.

    stmt = q.create_edge_by_names("Keanu", "TheMatrix", "acted_in", {"roles": ["Neo"]})
    assert stmt == "CREATE (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix)"
    stmt = q.create_edge_by_names("Keanu", "TheMatrix", "acted_in", {"roles": ["Neo", "The One"]})
    assert stmt == "CREATE (Keanu)-[:ACTED_IN {roles:['Neo', 'The One']}]->(TheMatrix)"
    stmt = q.create_edge_by_names("Keanu", "TheMatrix", "acted_in")
    assert stmt == "CREATE (Keanu)-[:ACTED_IN]->(TheMatrix)"
    stmt = q.create_edge_by_names("Keanu", "TheMatrix", "acted_in", edge_name="rel1")
    assert stmt == "CREATE (Keanu)-[rel1:ACTED_IN]->(TheMatrix)"
