from mlinspect.inspections._inspection_input import OperatorType
from mlinspect.inspections._lineage import LineageId

def to_entry(serialised):
    parts = serialised.split(',')
    return LineageId(int(parts[0]), int(parts[1]))


def to_polynomial(serialised):
    translation_table = dict.fromkeys(map(ord, '()'), None)
    no_brackets = serialised.translate(translation_table)
    tokens = no_brackets.split(";")
    return set([to_entry(token) for token in tokens])


def to_polynomials(series):
    return list(series.map(lambda serialised: to_polynomial(serialised)))
