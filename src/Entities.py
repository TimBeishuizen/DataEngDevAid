next_id_val = 0


def get_next_id() -> int:
    global next_id_val
    next_id_val += 1
    return next_id_val


class Activity:
    def __init__(self, identifier: str, description: str, status: int):
        self.identifier = identifier
        self.description = description
        self.status = status
        self.obj_id = get_next_id()

    identifier: str
    description: str
    status: int
    obj_id: int


class Budget:
    def __init__(self, period_start: str, period_end: str, value: int, value_date: str):
        self.period_start = period_start
        self.period_end = period_end
        self.value = value
        self.value_date = value_date
        self.obj_id = get_next_id()

    period_start: str
    period_end: str
    value: int
    value_date: str
    obj_id: int


class Disbursement:
    def __init__(self, period_start: str, period_end: str, value: int, value_date: str):
        self.period_start = period_start
        self.period_end = period_end
        self.value = value
        self.value_date = value_date
        self.obj_id = get_next_id()

    period_start: str
    period_end: str
    value: int
    value_date: str
    obj_id: int


class Organization:
    def __init__(self, name: str, ref: str, ty: int):
        self.name = name
        self.ref = ref
        self.type = ty
        self.obj_id = get_next_id()

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

    name: str
    vocabulary: int
    code: int
    significance: int
    obj_id: int


class Location:
    def __init__(self, code: str):
        self.code = code
        self.name = "R" + code if code.isnumeric() else code
        self.obj_id = get_next_id()

    code: str
    name: str
    obj_id: int
