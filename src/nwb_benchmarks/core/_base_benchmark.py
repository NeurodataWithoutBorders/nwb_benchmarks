import itertools
from typing import Any, Dict, Self, Union

import asv_runner


class BaseBenchmark:
    """
    The ASV convention is to specify parametrized fields as a pair of `param_names` and `params`.

    This was deemed less readable than a single dictionary of parameter cases.

    So inheriting from a `BaseBenchmark` allows us to specify parameter cases as the form...

    parameter_cases = dict(
        ParameterCase1=dict(
            parameter1=value1,
            parameter2=value2,
            ...
        ),
        ParameterCase2=dict(
            parameter1=value3,
            parameter2=value4,
            ...
        ),
        ...
    )

    ...and this will be unpacked into the expected form by ASV.
    """

    rounds = 1
    repeat = 1
    parameter_cases: Union[Dict[str, Dict[str, Any]], None] = None

    def __new__(cls, *args, **kwargs) -> Self:
        instance = super().__new__(cls)

        # Unpack parameter cases dictionary into ASV expected form
        if cls.parameter_cases is not None:
            cls.param_names = list(next(iter(cls.parameter_cases.values())).keys())
            cls.params = [
                [parameter_case[param_name] for parameter_case in cls.parameter_cases.values()]
                for param_name in cls.param_names
            ]

            # ASV automatically forms a cartesian product over all params
            # But we want our `parameter_names` usage to be flat in order to be more explicit
            # So use the skip decorator to exclude the 'off-diagonal' parts of a square product
            # These will still show up in the console display table, but will have n/a as display value
            # And values of samples in intermediate results will be saved to JSON as `null`
            cartesian_params = itertools.product(*cls.params)
            desired_params = [
                tuple(parameter_case[parameter_name] for parameter_name in cls.param_names)
                for parameter_case in cls.parameter_cases.values()
            ]
            non_cartesian_exclusion = [
                cartesian_param for cartesian_param in cartesian_params if cartesian_param not in desired_params
            ]

            for attr_name in dir(cls):
                attr = getattr(cls, attr_name)
                if callable(attr) and any(
                    attr_name.startswith(prefix) for prefix in ["time_", "track_"]
                ):  # Add more when needed
                    setattr(cls, attr_name, asv_runner.benchmarks.mark.skip_for_params(non_cartesian_exclusion)(attr))

        return instance
