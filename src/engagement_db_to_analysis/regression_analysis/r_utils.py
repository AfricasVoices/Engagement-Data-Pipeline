# TODO: Move to CoreDataModules once stable
from collections import defaultdict

from rpy2.robjects import DataFrame, FactorVector


def convert_dicts_to_r_data_frame_of_factors(dicts):
    """
    Converts a list of dictionaries to an R data-frame.

    Each column in the returned data-frame contains a "FactorVector", which is the datatype used to represent
    categorical data in R.

    :param dicts: Dictionaries to convert. Every dictionary must contain the same keys.
    :type dicts: dict of str -> (str | int | None)
    """
    if len(dicts) == 0:
        return DataFrame({})

    # Ensure every dict contains the same keys
    keys = set(dicts[0].keys())
    for d in dicts:
        assert set(d.keys()) == keys, f"{set(d.keys())}, {keys}"

    # R data-frames are constructed from a dict of (column_name) -> (values_vector), where
    # the nth row of the created data_frame contains the values at the nth position of each values_vector.
    # Therefore:

    # 1. Convert the input dicts to a dict of (column_name -> list of values)
    lists = defaultdict(list)
    for d in dicts:
        for (k, v) in d.items():
            lists[k].append(None if v is None else str(v))

    # 2. Convert each list of values to an R vector
    vectors = dict()
    for (dataset_name, values) in lists.items():
        vectors[dataset_name] = FactorVector(values)

    # 3. Convert the R vectors to an R data frame
    return DataFrame(vectors)
