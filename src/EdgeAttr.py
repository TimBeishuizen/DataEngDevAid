from typing import Dict, List, Any
from xml.etree import ElementTree as ET

try:
    from Entities import *
    from SessionExtension import SessionExtension
except ImportError:
    from .Entities import *
    from .SessionExtension import SessionExtension


class ActivityDate:
    START_PLANNED = 1
    START_ACTUAL = 2
    END_PLANNED = 3
    END_ACTUAL = 4


class EdgeAttr:
    @staticmethod
    def get_disbursements(node: ET.Element, parent_activity: Activity) -> List[Disbursement]:
        disbursements: List[Disbursement] = []
        for i, disbursement_node in enumerate(node.iter("planned-disbursement")):
            disbursement_node: ET.Element = disbursement_node
            period_start: str = disbursement_node.find("period-start").get("iso-date")
            period_end: str = disbursement_node.find("period-end").get("iso-date")
            value_node: ET.Element = disbursement_node.find("value")
            value: int = int(value_node.text)
            disbursement = Disbursement(period_start, period_end, value, parent_activity, i)
            disbursements.append(disbursement)
        return disbursements

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
            transaction = Transaction(ty, date, value, provider_ref, provider_name,
                                      receiver_ref, receiver_name, organizations)
            transactions.append(transaction)
        return transactions

    @staticmethod
    def commits(budget: Budget) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["period_start"] = budget._period_start
        attr_dict["period_end"] = budget._period_end
        return attr_dict

    @staticmethod
    def executed_in(activity: Activity) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.START_PLANNED, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.START_ACTUAL, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.END_PLANNED, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.END_ACTUAL, -1)
        return attr_dict

    @staticmethod
    def implements() -> Dict[str, Any]:
        return dict()

    @staticmethod
    def transacts(transaction: Transaction) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["type"] = transaction.type
        attr_dict["date"] = transaction.date
        attr_dict["value"] = transaction.value
        return attr_dict

    @staticmethod
    def plans_disbursement(disbursement: Disbursement) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["period_start"] = disbursement.period_start
        attr_dict["period_end"] = disbursement.period_end
        attr_dict["value"] = disbursement.value
        return attr_dict

    @staticmethod
    def supports(activity: Activity, policy: Policy, policy_significance_map: Dict[int, int]) -> Dict[str, Any]:
        def get_pol_sig(code: int) -> int:
            return policy_significance_map.get(code, 0)

        attr_dict: Dict[str, Any] = dict()
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.START_PLANNED, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.START_ACTUAL, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.END_PLANNED, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.END_ACTUAL, -1)
        attr_dict["significance"] = get_pol_sig(policy.code)
        return attr_dict

    @staticmethod
    def participates_in(activity: Activity) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.START_PLANNED, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.START_ACTUAL, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.END_PLANNED, -1)
        attr_dict["planned_period_start"] = activity.dates.get(ActivityDate.END_ACTUAL, -1)
        return attr_dict

    @staticmethod
    def funds(budget: Budget) -> Dict[str, Any]:
        attr_dict: Dict[str, Any] = dict()
        attr_dict["period_start"] = budget._period_start
        attr_dict["period_end"] = budget._period_end
        return attr_dict
