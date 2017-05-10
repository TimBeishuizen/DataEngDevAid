import xml.etree.ElementTree as ET

# Can also add the other files. I can't sync them yet
tree = ET.ElementTree(file='../data/IATIACTIVITIES19972007.xml')

# Important, a lot of the elements have a child named narrative/value containing the name/value under text

# Find all names of several parts of the project
# Participating org (project): "//participating-org[@role='4']"
# Reporting org (project): "//reporting-org"
# Provider org (transaction): "//provider-org"
# Receiver org (transaction): "//receiver-org"
# Recipient region (project): "//recipient-region"
# Recipient country (project): "//recipient-country"
# Sector (project): "//sector"
# Policy marker (project): "//policy-marker"

indent = 0
TAB_SPACE = 4
for elem in tree.getroot()[0]:
    elem_text: str = elem.text
    if elem_text.isspace():
        print(elem.tag, elem.attrib)
    else:
        print(elem.tag, elem.attrib, elem.text)
    indent += TAB_SPACE
    for child_elem in elem.findall("./*"):
        child_text: str = child_elem.text
        if child_text.isspace():
            print("" * indent, child_elem.tag, child_elem.attrib)
        else:
            print("" * indent, child_elem.tag, child_elem.attrib, child_elem.text)
    indent -= TAB_SPACE
