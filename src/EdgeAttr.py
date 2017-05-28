from typing import Dict, Any
from xml.etree import ElementTree as ET

if __name__ == "__main__":
    from Entities import *
    from SessionExtension import SessionExtension
else:
    from .Entities import *
    from .SessionExtension import SessionExtension


class EdgeAttr:
    @staticmethod
    def get_activity_attributes(activity: Activity) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        for act_date in activity.dates:
            key = "date_type_{}".format(act_date.type)
            attr_dict[key] = act_date.date
        return attr_dict

    @staticmethod
    def get_disbursements(node: ET.Element, parent_activity: Activity) -> List[Disbursement]:
        disbursements: List[Disbursement] = []
        for i, disbursement_node in enumerate(node.iter("planned-disbursement")):
            disbursement_node: ET.Element = disbursement_node
            period_start: str = disbursement_node.find("period-start").get("iso-date")
            period_end: str = disbursement_node.find("period-end").get("iso-date")
            value_node: ET.Element = disbursement_node.find("value")
            value: int = int(value_node.text)
            value_date: str = value_node.get("value-date")
            disbursement = Disbursement(period_start, period_end, value, value_date, parent_activity, i)
            disbursements.append(disbursement)
        return disbursements

    @staticmethod
    def get_disbursement_attributes(disbursement: Disbursement) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["period_start"] = disbursement.period_start
        attr_dict["period_end"] = disbursement.period_end
        attr_dict["value"] = disbursement.value
        attr_dict["value_date"] = disbursement.value_date
        return attr_dict

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

    @staticmethod
    def get_transaction_attributes(transaction: Transaction) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["type"] = transaction.type
        attr_dict["date"] = transaction.date
        attr_dict["value"] = transaction.value
        attr_dict["value_date"] = transaction.value_date
        attr_dict["provider_ref"] = transaction.provider_ref
        attr_dict["provider_name"] = transaction.provider_name
        attr_dict["receiver_ref"] = transaction.receiver_ref
        attr_dict["receiver_name"] = transaction.receiver_name
        return attr_dict
