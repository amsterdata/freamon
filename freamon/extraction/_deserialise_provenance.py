from mlinspect.inspections._lineage import LineageId


def to_polynomials(series):
    polynomials = []
    for serialised in series.values:
        polynomial = set()
        for token in serialised[1:len(serialised)-1].split(");("):
            parts = token.split(',')
            polynomial.add(LineageId(int(parts[0]), int(parts[1])))
        polynomials.append(polynomial)

    return polynomials
