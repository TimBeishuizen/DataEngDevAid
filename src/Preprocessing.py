import xml.etree.ElementTree as ET
import csv

# Can also add the other files. I can't sync them yet
tree9707 = ET.ElementTree(file='../data/IATIACTIVITIES19972007.xml')
tree0809 = ET.ElementTree(file='../data/IATIACTIVITIES20082009.xml')
tree1011 = ET.ElementTree(file='../data/IATIACTIVITIES20102011.xml')
tree1213 = ET.ElementTree(file='../data/IATIACTIVITIES20122013.xml')
tree1415 = ET.ElementTree(file='../data/IATIACTIVITIES20142015.xml')
tree1617 = ET.ElementTree(file='../data/IATIACTIVITIES20162017.xml')
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


def write_activities(tree, writer):
    for elem in tree.findall(".//iati-activity"):
        writer.writerow([elem.find("title/narrative").text,
                         elem.find("description/narrative").text,
                         #elem.find("other-identifier").get("type"),
                         elem.find("activity-status").get("code")])

with open('activities.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=' ',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)

    write_activities(tree9707, writer)
    write_activities(tree0809, writer)
    write_activities(tree1011, writer)
    write_activities(tree1213, writer)
    write_activities(tree1415, writer)
    write_activities(tree1617, writer)







