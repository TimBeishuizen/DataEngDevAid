import re
from typing import Iterable, Dict

next_id_val = 0

R_CHARS = re.compile(r"[.()\[\]' \-*,/&\":;%@#$<>!?|+={}~`^]")

TRANSACTION_DEBUG = False


def get_next_id() -> int:
    global next_id_val
    next_id_val += 1
    return next_id_val


def date_str_to_int(date_str: str) -> int:
    return int(date_str.replace("-", ""))


def sanitize_date(date_val: int) -> int:
    return 20170630 if date_val == -1 or date_val > 99990000 else date_val


class Activity:
    class ActivityDate:
        def __init__(self, ty: int, date: str):
            self.type = ty
            self.date = date_str_to_int(date)

        type: int
        date: int

    def __init__(self, identifier: str, description: str, status: int, title: str, dates: Iterable[ActivityDate]):
        self.identifier = identifier
        self.description = description
        self.title = title
        self.status = status
        self.obj_id = get_next_id()
        self.dates = dict()
        for date in dates:
            self.dates[date.type] = date.date

    def get_name(self) -> str:
        return "act_" + self.identifier.replace("-", "_")

    identifier: str
    description: str
    title: str
    status: int
    obj_id: int
    # For edges
    dates: Dict[int, int]


class Budget:
    def __init__(self, period_start: str, period_end: str, value: int, ty: int, status: int,
                 parent_activity: Activity):
        self.value = value
        self.type = ty
        self.status = status
        self.obj_id = get_next_id()
        self._parent_activity = parent_activity
        self._period_start = date_str_to_int(period_start)
        self._period_end = date_str_to_int(period_end)

    def get_name(self) -> str:
        return "bud_" + self._parent_activity.identifier.replace("-", "_")

    value: int
    type: int
    status: int
    obj_id: int
    _parent_activity: Activity
    _period_start: int
    _period_end: int


class Disbursement:
    def __init__(self, period_start: str, period_end: str, value: int, parent_activity: Activity,
                 index: int):
        self.period_start = date_str_to_int(period_start)
        self.period_end = date_str_to_int(period_end)
        self.value = value
        self.obj_id = get_next_id()
        self._parent_activity = parent_activity
        self._index = index

    def get_name(self) -> str:
        return "dis_" + self._parent_activity.identifier.replace("-", "_") + "_{}".format(self._index)

    period_start: int
    period_end: int
    value: int
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
        ref = R_CHARS.sub("_", name) if ref is None else ref
        if re.match(r"^[^A-Za-z_]", ref):
            ref = "org_" + ref
        return ref

    name: str
    ref: str
    type: int
    obj_id: int
    # 'role' should be a property of 'participates' relation?


class Policy:
    def __init__(self, name: str, code: int):
        self.name = name
        self.code = code
        self.obj_id = get_next_id()

    def get_name(self) -> str:
        return Policy.get_unique_name(self.code)

    @staticmethod
    def get_unique_name(code: int) -> str:
        return "pol_" + str(code)

    name: str
    code: int
    obj_id: int


class Location:
    def __init__(self, code: str, name: str):
        self.code = code
        self.name = name
        self.is_country = not code.isnumeric()
        self.obj_id = get_next_id()

    def get_name(self) -> str:
        n = "region-" + self.code if self.code.isnumeric() else "country-" + self.code
        return n.replace("-", "_")

    code: str
    name: str
    is_country: bool
    obj_id: int


class Transaction:
    def __init__(self, ty: int, date: str, value: int, provider_ref: str, provider_name: str,
                 receiver_ref: str, receiver_name: str, orgs: Iterable[Organization] = None):
        self.type = ty
        self.date = date_str_to_int(date)
        self.value = value
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
                    if TRANSACTION_DEBUG:
                        print("[WARN] Cannot find responsible organization(s) for transaction: "
                              "date={}, provider={}, receiver={}; key={}"
                              .format(date, provider_name, receiver_name, key))
                    org = None
                return org

            self.provider_org = find_org(orgs, self.provider_ref, self.provider_name)
            self.receiver_org = find_org(orgs, self.receiver_ref, self.receiver_name)

    type: int
    date: int
    value: int
    provider_ref: str
    provider_name: str
    receiver_ref: str
    receiver_name: str
    provider_org: Organization
    receiver_org: Organization
