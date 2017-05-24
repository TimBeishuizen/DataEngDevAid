import re
from typing import Iterable, List

next_id_val = 0

R_CHARS: str = r"[.()\[\]' \-*,/&\":]"


def get_next_id() -> int:
    global next_id_val
    next_id_val += 1
    return next_id_val


class Activity:
    class ActivityDate:
        def __init__(self, ty: int, date: str):
            self.type = ty
            self.date = date

        type: int
        date: str

    def __init__(self, identifier: str, description: str, status: int, dates: Iterable[ActivityDate]):
        self.identifier = identifier
        self.description = description
        self.status = status
        self.obj_id = get_next_id()
        self.dates = list(dates)

    def get_name(self) -> str:
        return "act_" + self.identifier.replace("-", "_")

    identifier: str
    description: str
    status: int
    obj_id: int
    # For edges
    dates: List[ActivityDate]


class Budget:
    def __init__(self, period_start: str, period_end: str, value: int, value_date: str, parent_activity: Activity):
        self.period_start = period_start
        self.period_end = period_end
        self.value = value
        self.value_date = value_date
        self.obj_id = get_next_id()
        self._parent_activity = parent_activity

    def get_name(self) -> str:
        return "bud_" + self._parent_activity.identifier.replace("-", "_")

    period_start: str
    period_end: str
    value: int
    value_date: str
    obj_id: int
    _parent_activity: Activity


class Disbursement:
    def __init__(self, period_start: str, period_end: str, value: int, value_date: str, parent_activity: Activity,
                 index: int):
        self.period_start = period_start
        self.period_end = period_end
        self.value = value
        self.value_date = value_date
        self.obj_id = get_next_id()
        self._parent_activity = parent_activity
        self._index = index

    def get_name(self) -> str:
        return "dis_" + self._parent_activity.identifier.replace("-", "_") + "_{}".format(self._index)

    period_start: str
    period_end: str
    value: int
    value_date: str
    obj_id: int
    _parent_activity: Activity
    _index: int


class Organization:
    def __init__(self, name: str, ref: str, ty: int):
        self.name = name
        self.ref = ref
        self.type = ty
        self.obj_id = get_next_id()

    def get_name(self) -> str:
        ref = Organization.get_unique_ref(self.name, self.ref)
        return ref.replace("-", "_")

    @staticmethod
    def get_unique_ref(name: str, ref: str) -> str:
        # Some organizations, like Steps Towards Development, do not have a ref.
        ref = re.sub(R_CHARS, "_", name) if ref is None else ref
        if re.match(r"^[^A-Za-z_].*", ref):
            ref = "org_" + ref
        return ref

    name: str
    ref: str
    type: int
    obj_id: int
    # 'role' should be a property of 'participates' relation?


class Policy:
    def __init__(self, name: str, vocabulary: int, code: int, significance: int):
        self.name = name
        self.vocabulary = vocabulary
        self.code = code
        self.significance = significance
        self.obj_id = get_next_id()

    def get_name(self) -> str:
        return re.sub(R_CHARS, "_", self.name)

    name: str
    vocabulary: int
    code: int
    significance: int
    obj_id: int


class Location:
    def __init__(self, code: str):
        self.code = code
        self.name = "region-" + code if code.isnumeric() else "country-" + code
        self.obj_id = get_next_id()

    def get_name(self) -> str:
        return self.name.replace("-", "_")

    code: str
    name: str
    obj_id: int


class Transaction:
    def __init__(self, ty: int, date: str, value: int, value_date: str, provider_ref: str, provider_name: str,
                 receiver_ref: str, receiver_name: str, orgs: Iterable[Organization] = None):
        self.type = ty
        self.date = date
        self.value = value
        self.value_date = value_date
        self.provider_ref = provider_ref
        self.receiver_ref = receiver_ref
        self.provider_name = provider_name
        self.receiver_name = receiver_name
        if orgs is not None:
            def find_org(orgs: Iterable[Organization], ref: str, name: str) -> Organization:
                key = Organization.get_unique_ref(name, ref).replace("-", "_")
                try:
                    org = next(x for x in orgs if x.get_name() == key)
                except StopIteration:
                    print("[WARN] Cannot find responsible organization(s) for transaction: "
                          "date={}, provider={}, receiver={}; key={}"
                          .format(date, provider_name, receiver_name, key))
                    org = None
                return org

            self.provider_org = find_org(orgs, self.provider_ref, self.provider_name)
            self.receiver_org = find_org(orgs, self.receiver_ref, self.receiver_name)

    type: int
    date: str
    value: int
    value_date: str
    provider_ref: str
    provider_name: str
    receiver_ref: str
    receiver_name: str
    provider_org: Organization
    receiver_org: Organization